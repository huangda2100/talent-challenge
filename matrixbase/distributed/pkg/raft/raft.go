package raft

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/pkg/v3/fileutil"
	"go.etcd.io/etcd/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.uber.org/zap"
)

type raftNode struct {
	id               int
	peers            []string
	join             bool //new node or not
	walDir           string
	snapshotDir      string
	lastIndex        uint64 // log start index
	snapshotIndex    uint64
	appliedIndex     uint64
	node             raft.Node
	raftStorage      *raft.MemoryStorage
	wal              *wal.WAL
	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready
	confState        raftpb.ConfState

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	proposeChan    <-chan string            //k-v message
	confChangeChan <-chan raftpb.ConfChange //cluster config
	commitChan     chan<- *string           // entries are committed to logs
	errorChan      chan<- error

	getSnapshot func() ([]byte, error)
}

var defaultSnapshotCount uint64 = 10000

func NewRaftNode(id int, peers []string, join bool, proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange, getSnapshot func() ([]byte, error)) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {

	commitC := make(chan *string)
	errorC := make(chan error)

	rc := &raftNode{
		proposeChan:    proposeC,
		confChangeChan: confChangeC,
		commitChan:     commitC,
		errorChan:      errorC,
		id:             id,
		peers:          peers,
		join:           join,
		walDir:         fmt.Sprintf("matrixbase-%d", id),
		snapshotDir:    fmt.Sprintf("matrixbase-%d-snap", id),
		getSnapshot:    getSnapshot,
		snapCount:      defaultSnapshotCount,
		stopc:          make(chan struct{}),
		httpstopc:      make(chan struct{}),
		httpdonec:      make(chan struct{}),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}

	log.Println("NewRaftNode")
	go rc.startRaft()
	return commitC, errorC, rc.snapshotterReady
}

func(rn *raftNode) startRaft() {
	log.Println("startRaft1")
	if !fileutil.Exist(rn.snapshotDir) {
		if err := os.Mkdir(rn.snapshotDir, 0750); err != nil {
			log.Fatalf("matrixbase: cannot create dir for snapshot (%v)", err)
		}
	}
	log.Println("startRaft2")
	rn.snapshotter = snap.New(zap.NewExample(), rn.snapshotDir)
	rn.snapshotterReady <- rn.snapshotter

	//oldWal := wal.Exist(rn.walDir)
	rn.wal = rn.replayWAL()

	rpeers := make([]raft.Peer, len(rn.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	conf := &raft.Config{
		ID:                        uint64(rn.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rn.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}
	rn.node = raft.StartNode(conf, rpeers)

	//if oldWal {
	//	log.Println("RestartNode")
	//	rn.node = raft.RestartNode(conf)
	//} else {
	//	startPeers := rpeers
	//	if rn.join {
	//		startPeers = nil
	//	}
	//	log.Println("raft.StartNode")
	//	rn.node = raft.StartNode(conf, startPeers)
	//}

	rn.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rn.id),
		ClusterID:   0x1000,
		Raft:        rn,
		ServerStats: v2stats.NewServerStats("", ""),
		LeaderStats: v2stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rn.id)),
		ErrorC:      make(chan error),
	}

	rn.transport.Start()
	for i := range rn.peers {
		if i+1 != rn.id {
			rn.transport.AddPeer(types.ID(i+1), []string{rn.peers[i]})
		}
	}

	go rn.serveRaft()
	go rn.serveChannels()
}

func (rn *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rn.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("matrixbase: error loading snapshot (%v)", err)
	}
	return snapshot
}

func (rn *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rn.walDir) {
		if err := os.Mkdir(rn.walDir, 0750); err != nil {
			log.Fatalf("matrixbase: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rn.walDir, nil)
		if err != nil {
			log.Fatalf("matrixbase: create wal error (%v)", err)
		}
		w.Close()
	}

	walSnap := walpb.Snapshot{}
	if snapshot != nil {
		walSnap.Index, walSnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	log.Printf("loading WAL at term %d and index %d", walSnap.Term, walSnap.Index)
	w, err := wal.Open(zap.NewExample(), rn.walDir, walSnap)
	if err != nil {
		log.Fatalf("matrixbase: error loading wal (%v)", err)
	}

	return w
}

//when to launch
func (rn *raftNode) replayWAL() *wal.WAL {
	snapshot := rn.loadSnapshot()
	w := rn.openWAL(snapshot)
	_, st, ents, err := w.ReadAll() //state entries
	if err != nil {
		log.Fatalf("matrixbase: failed to read WAL (%v)", err)
	}

	rn.raftStorage = raft.NewMemoryStorage() // why don't do this on newRaftNode?
	if snapshot != nil {
		rn.raftStorage.ApplySnapshot(*snapshot)
	}
	rn.raftStorage.SetHardState(st)

	rn.raftStorage.Append(ents)
	if len(ents) > 0 {
		rn.lastIndex = ents[len(ents) - 1].Index
	} else {
		rn.commitChan <- nil
	}

	return w
}

func (rn *raftNode) serveRaft() {
	urlAddress, err := url.Parse(rn.peers[rn.id - 1])
	if err != nil {
		log.Fatalf("matrixbase: Failed parsing URL (%v)", err)
	}
	log.Println("serveRaft urlAddress:", urlAddress)
	ln, err := newStoppableListener(urlAddress.Host, rn.httpstopc)
	if err != nil {
		log.Fatalf("matrixbase: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rn.transport.Handler()}).Serve(ln)
	select {
	case <-rn.httpstopc:
	default:
		log.Fatalf("matrixbase: Failed to serve rafthttp (%v)", err)
	}
	close(rn.httpdonec)
}

func (rc *raftNode) serveChannels() {
	snapshotVal, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snapshotVal.Metadata.ConfState
	rc.snapshotIndex = snapshotVal.Metadata.Index
	rc.appliedIndex = snapshotVal.Metadata.Index

	defer rc.wal.Close()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	go func(){
		confChangeCount := uint64(0)
		for rc.proposeChan != nil && rc.confChangeChan != nil {
			select {
			case propose, ok := <- rc.proposeChan:
				if !ok {
					rc.proposeChan = nil
				} else {
					log.Println("propose msg")
					rc.node.Propose(context.TODO(), []byte(propose))
				}
			case cc, ok := <-rc.confChangeChan:
				if !ok {
					rc.confChangeChan = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}

		close(rc.stopc)
	}()

	for {
		select {
		case <- ticker.C:
			//heartbeat and election tick
			rc.node.Tick()
		case rd := <- rc.node.Ready():
			//when will data be written into this chan
			rc.wal.Save(rd.HardState,rd.Entries)
			//log.Println("save wal")
			if !raft.IsEmptySnap(rd.Snapshot) {
				log.Println("saveSnap")
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot()
			rc.node.Advance()
		}
	}
}

func (rn *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			select {
			case rn.commitChan <- &s:
			case <-rn.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rn.confState = *rn.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rn.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rn.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rn.transport.RemovePeer(types.ID(cc.NodeID))
			}

		}

		// after commit, update appliedIndex
		rn.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rn.lastIndex {
			select {
			case rn.commitChan <- nil:
			case <-rn.stopc:
				return false
			}
		}
	}

	return true
}

func (rn *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rn.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rn.appliedIndex)
	}
	if rn.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rn.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (rn *raftNode) stop() {
	rn.stopHTTP()
	close(rn.commitChan)
	close(rn.errorChan)
	rn.node.Stop()
}

func (rn *raftNode) stopHTTP() {
	rn.transport.Stop()
	close(rn.httpstopc)
	<-rn.httpdonec
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rn *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes. But why?
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rn.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}

	if err := rn.snapshotter.SaveSnap(snap); err != nil {
		return err
	}

	return rn.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rn *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rn.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rn.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rn.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rn.appliedIndex)
	}
	rn.commitChan <- nil // trigger kvstore to load snapshot

	rn.confState = snapshotToSave.Metadata.ConfState
	rn.snapshotIndex = snapshotToSave.Metadata.Index
	rn.appliedIndex = snapshotToSave.Metadata.Index
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
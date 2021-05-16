package store

import (
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/raft"
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/store/pebble_store"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"strings"
)

// Store the store interface
type Store interface {
	// Set set key-value to store
	Set(key []byte, value []byte) error
	// Get returns the value from store
	Get(key []byte) ([]byte, error)
	// Delete remove the key from store
	Delete(key []byte) error
}

func NewStore(cfg cfg.StoreCfg) (Store, error) {
	if cfg.Memory {
		return newMemoryStore()
	}

	cfg.ClusterIp = "http://172.19.1.1:7001,http://172.19.1.2:7002,http://172.19.1.3:7003"
	cfg.Join = false

	proposeChan := make(chan string)
	//defer close(proposeChan)
	confChangeChan := make(chan raftpb.ConfChange)
	snapshotReady := make(chan *snap.Snapshotter, 1)
	//defer close(confChangeChan)

	var ps *pebble_store.PebbleStorage
	getSnapshot := func() ([]byte, error) {return ps.GetSnapshot()}
	commitChan, errorChan := raft.NewRaftNode(cfg.Id, strings.Split(cfg.ClusterIp, ","), cfg.Join, proposeChan, confChangeChan, snapshotReady, getSnapshot)
	ps = pebble_store.NewPebbleStorage(<-snapshotReady,proposeChan)
	ps.StartReadCommit(commitChan, errorChan)

	// TODO: need to implement
	return ps, nil
}

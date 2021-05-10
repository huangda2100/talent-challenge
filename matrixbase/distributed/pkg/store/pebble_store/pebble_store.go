package pebble_store

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"log"
	"sync"
)

type PebbleStorage struct {
	mu sync.RWMutex
	proposeChan chan<- string
	snapshot *snap.Snapshotter
	db *pebble.DB
}

type kv struct {
	Key string
	Val string
}

func NewPebbleStorage(snapshot *snap.Snapshotter, proposeChan chan<- string, commitChan <-chan *string, errorChan <-chan error) *PebbleStorage {
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		log.Fatal(err)
	}

	ps := &PebbleStorage{
		db: db,
		proposeChan: proposeChan,
		snapshot: snapshot,
	}

	// replay log into key-value map, the reason is:?
	ps.readCommits(commitChan, errorChan)
	// read commits from raft into kvStore map until error
	go ps.readCommits(commitChan, errorChan)
	return ps
}

func (s *PebbleStorage) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapshot.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()
		s.db.Set([]byte(dataKv.Key), []byte(dataKv.Val), pebble.Sync)
		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *PebbleStorage) GetSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := s.db.NewSnapshot()
	defer snap.Close()

	iter := snap.NewIter(nil)
	defer iter.Close()

	var kvs = make(map[string]string)
	for iter.First(); iter.Valid(); iter.Next() {
		kvs[string(iter.Key())] = string(iter.Value())
	}
	return json.Marshal(kvs)
}

// 1.application layer get snapshot and deliver the byte data to etcd server
// 2.etcd server save snapshot data
// 3.this method recover the data which is stored in step 2
func (s *PebbleStorage) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k,v := range store {
		s.db.Set([]byte(k), []byte(v), pebble.Sync)
	}
	return nil
}

func (ps *PebbleStorage) Set(key []byte, value []byte) error {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	ps.db.Set(key, value, pebble.Sync)

	return nil
}

func (ps *PebbleStorage) Get(key []byte) ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	v, closer, err := ps.db.Get(key)
	closer.Close()

	return v, err
}

func (ps *PebbleStorage) Delete(key []byte) error {
	return ps.db.Delete(key, nil)
}

/*func (ps *PebbleStorage) ApplySnapshot(snap raftpb.Snapshot) error {
	ps.Lock()
	defer ps.Unlock()

	psIndex := ps.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if psIndex >= snapIndex {
		//the snapshot which need to be applied is out of date
		return ErrSnapOutOfDate
	}

	ps.snapshot = snap
	ps.ents = []raftpb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

func (ps *PebbleStorage) SetHardState(st raftpb.HardState) error {
	ps.Lock()
	defer ps.Unlock()
	ps.hardState = st
	return nil
}

func (ps *PebbleStorage) firstIndex() uint64 {
	return ps.ents[0].Index + 1 //ents[0] is dummy entry
}

func (ps *PebbleStorage) lastIndex() uint64 {
	return ps.ents[0].Index + uint64(len(ps.ents)) - 1
}

func (ps *PebbleStorage) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ps.Lock()
	defer ps.Unlock()

	first := ps.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	if last < first {
		return nil //the biggest index entry is exist already
	}

	if first > entries[0].Index {
		entries = entries[first - entries[0].Index:]
	}

	//first index: 3
	//entries[0].Index: 1
	//entries[2:]
	//offset: 0 so why?

	offset := entries[0].Index - ps.ents[0].Index
	switch {
	case uint64(len(ps.ents)) > offset:
		ps.ents = append([]raftpb.Entry{}, ps.ents[:offset]...)
		ps.ents = append(ps.ents, entries...)
	case uint64(len(ps.ents)) == offset:
		ps.ents = append(ps.ents, entries...)
	default:
		log.Panicf("missing log entry [last: %d, append at: %d]",
			ps.lastIndex(), entries[0].Index)
	}
	return nil
}

//implement raft.config
func (ps *PebbleStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return ps.hardState, ps.snapshot.Metadata.ConfState, nil
}

func (ps *PebbleStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	ps.Lock()
	defer ps.Unlock()
	offset := ps.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ps.lastIndex()+1 {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ps.lastIndex())
	}
	// only contains dummy entries.
	if len(ps.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := ps.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), nil
}

func (ps *PebbleStorage) Term(i uint64) (uint64, error) {
	ps.Lock()
	defer ps.Unlock()
	offset := ps.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ps.ents) {
		return 0, ErrUnavailable
	}
	return ps.ents[i-offset].Term, nil
}

func (ps *PebbleStorage) LastIndex() (uint64, error) {
	ps.Lock()
	defer ps.Unlock()
	return ps.lastIndex(), nil
}

func (ps *PebbleStorage) FirstIndex() (uint64, error) {
	ps.Lock()
	defer ps.Unlock()
	return ps.firstIndex(), nil
}

func (ps *PebbleStorage) Snapshot() (raftpb.Snapshot, error) {
	ps.Lock()
	defer ps.Unlock()
	return ps.snapshot, nil
}

func (ps *PebbleStorage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	ps.Lock()
	defer ps.Unlock()
	if i <= ps.snapshot.Metadata.Index {
		return raftpb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ps.ents[0].Index
	if i > ps.lastIndex() {
		log.Panicf("snapshot %d is out of bound lastindex(%d)", i, ps.lastIndex())
	}

	ps.snapshot.Metadata.Index = i
	ps.snapshot.Metadata.Term = ps.ents[i-offset].Term
	if cs != nil {
		ps.snapshot.Metadata.ConfState = *cs
	}
	ps.snapshot.Data = data
	return ps.snapshot, nil
}

func (ps *PebbleStorage) Compact(compactIndex uint64) error {
	ps.Lock()
	defer ps.Unlock()
	offset := ps.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ps.lastIndex() {
		log.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ps.lastIndex())
	}

	i := compactIndex - offset
	ents := make([]raftpb.Entry, 1, 1+uint64(len(ps.ents))-i)
	ents[0].Index = ps.ents[i].Index
	ents[0].Term = ps.ents[i].Term
	ents = append(ents, ps.ents[i+1:]...)
	ps.ents = ents
	return nil
}*/
package pebble_store

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/pebble"
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

var db *pebble.DB
var err error
func init() {
	db, err = pebble.Open("pebble_store", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
}

func NewPebbleStorage(snapshot *snap.Snapshotter, proposeChan chan<- string) *PebbleStorage {
	ps := &PebbleStorage{
		db: db,
		proposeChan: proposeChan,
		snapshot: snapshot,
	}

	return ps
}

func (s *PebbleStorage) StartReadCommit(commitChan <-chan *string, errorChan <-chan error) {
	// replay log into key-value map, the reason is:?
	s.readCommits(commitChan, errorChan)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitChan, errorChan)
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

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{string(key), string(value)}); err != nil {
		log.Fatal(err)
	}

	ps.proposeChan <- buf.String()

	return nil
}

func (ps *PebbleStorage) Get(key []byte) ([]byte, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	v, closer, err := ps.db.Get(key)
	if err != nil {
		fmt.Println("ps get err:", err)
	}
	closer.Close()

	return v, err
}

func (ps *PebbleStorage) Delete(key []byte) error {
	return ps.db.Delete(key, nil)
}
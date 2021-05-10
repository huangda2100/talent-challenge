package store

import (
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/raft"
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/store/pebble_store"
	"go.etcd.io/etcd/raft/v3/raftpb"
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

// NewStore create the raft store
func NewStore(cfg cfg.StoreCfg) (Store, error) {
	if cfg.Memory {
		return newMemoryStore()
	}

	proposeChan := make(chan string)
	defer close(proposeChan)
	confChangeChan := make(chan raftpb.ConfChange)
	defer close(confChangeChan)

	var pebbleStore pebble_store.PebbleStorage
	getSnapshot := func() ([]byte, error) {return pebbleStore.GetSnapshot()}
	commitChan, errorChan, snapshotReady := raft.NewRaftNode(cfg.Id, strings.Split(cfg.ClusterIp, ","), cfg.Join, proposeChan, confChangeChan, getSnapshot)

	ps := pebble_store.NewPebbleStorage(<-snapshotReady,proposeChan,commitChan,errorChan)

	// TODO: need to implement
	return ps, nil
}

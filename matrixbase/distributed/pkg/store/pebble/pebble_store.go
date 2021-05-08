package pebble

import (
	"errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"sync"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

type PebbleStorage struct {
	sync.Mutex

	hardState raftpb.HardState
	snapshot  raftpb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []raftpb.Entry
}

func NewPebbleStorage() *PebbleStorage {
	return &PebbleStorage{}
}

func (ps *PebbleStorage) Set(key []byte, value []byte) error {
	return nil
}

func (ps *PebbleStorage) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (ps *PebbleStorage) Delete(key []byte) error {
	return nil
}

func (ps *PebbleStorage) ApplySnapshot(snap raftpb.Snapshot) error {
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
}
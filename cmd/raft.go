package main

import (
	"encoding/binary"
	"encoding/json"
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
	"io"
)

// FSM is implemented by clients to make use of the replicated log.
type raftFSM struct {
	db     *badger.DB
	logger *zap.Logger
}

const (
	get string = "GET"
	set string = "SET"
	upd string = "UPD"
	del string = "DEL"
)

type event struct {
	opType string
	key    uint64
	value  uint64
}

func (e *event) Key() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, e.key)
	return b
}

func (e *event) Value() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, e.value)
	return b
}

// Apply is called once a log entry is committed by a majority of the cluster.
//
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (f *raftFSM) Apply(log *raft.Log) interface{} {
	var e event
	if err := json.Unmarshal(log.Data, &e); err != nil {
		f.logger.Fatal("Failed unmarshalling Log entry, this is a bug")
	}
	switch e.opType {
	case set, upd:
		err := f.db.Update(func(txn *badger.Txn) error {
			// TODO handle conflict here
			err := txn.Set(e.Key(), e.Value())
			return err
		})
		if err != nil {
			return err
		}
		// should read only operations go through raft?
	case del:
		err := f.db.Update(func(txn *badger.Txn) error {
			err := txn.Delete(e.Key())
			return err
		})
		if err != nil {
			return err
		}
	default:
		f.logger.Fatal("Unknown Operation found, could not apply")
	}
	return nil
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
//
// The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running. Generally this means Snapshot should
// only capture a pointer to the state, and any expensive IO should happen
// as part of FSMSnapshot.Persist.
//
// Apply and Snapshot are always called from the same thread, but Apply will
// be called concurrently with FSMSnapshot.Persist. This means the FSM should
// be implemented to allow for concurrent updates while a snapshot is happening.
func (f *raftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &raftFSMSnapshot{}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (*raftFSM) Restore(io.ReadCloser) error {
	return nil
}

// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent calls to Apply.
type raftFSMSnapshot struct {
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (*raftFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// Persist and Restore functions are related
	// raft persists the snapshot periodically
	// in case of complete failure the state is restored from disk
	// from the last time we Persisted, we don't need that since badger handles the persistence for us
	return nil
}

// Release is invoked when we are finished with the snapshot.
func (*raftFSMSnapshot) Release() {
}

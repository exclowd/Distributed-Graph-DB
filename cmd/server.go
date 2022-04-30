package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	// "strconv"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"
)

type config struct {
	id     string
	path   string
	addr   string
	leader bool
}

// The full server encapsulated in a struct
type server struct {
	cfg    *config
	logger *zap.Logger // logger
	raft   *raft.Raft  // the raft
	fsm    *raftFSM    // the fsm
	db     *badger.DB
}

func (s *server) get(key string) (string, error) {
	// keyS := strconv.FormatUint(key, 10)
	keyS := key
	var valS string

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyS))
		if err != nil {
			s.logger.Info("Key not available")
			return err
		}

		err = item.Value(func(val []byte) error {
			//s.logger.Info("Value that I got", zap.String("vals", string(val)))
			valCopy := append([]byte{}, val...)
			// valS, err = strconv.ParseUint(string(valCopy), 10, 64)
			valS = string(valCopy)
			return nil
		})

		return err
	})
	if err != nil {
		return "0", err
	}
	return valS, err
}

func (s *server) put(key, val string) error {

	data := event{
		OpType: "SET",
		Key:    key,
		Value:  val,
	}

	dataJson, err := json.Marshal(data)

	if err != nil {
		s.logger.Error("Could not marshal data")
	}

	applyFuture := s.raft.Apply(dataJson, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		s.logger.Error("Could not apply put method", zap.Error(err))
	}

	return nil
}

func (s *server) delete(key string) error {

	data := event{
		OpType: "SET",
		Key:    key,
		Value:  "0",
	}

	dataJson, err := json.Marshal(data)

	if err != nil {
		s.logger.Error("Could not marshal data")
	}

	applyFuture := s.raft.Apply(dataJson, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		s.logger.Error("Could not apply put method", zap.Error(err))
	}
	return nil
}

// respond to join requests by a node at joinAddr
func (s *server) join(joinAddr, id string) error {
	cfgFuture := s.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		s.logger.Fatal("failed to get raft configuration")
		return err
	}
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(id) || srv.Address == raft.ServerAddress(joinAddr) {
			if srv.ID == raft.ServerID(id) && srv.Address == raft.ServerAddress(joinAddr) {
				return nil
			}
			// This would be replicated to all nodes
			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				s.logger.Fatal("Failed to remove server")
				return err
			}
		}
	}
	// again replicated to all nodes part of the server
	f := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(joinAddr), 0, 0)
	if f.Error() != nil {
		fmt.Println(f.Error())
		return f.Error()
	}
	// must have joined successfully
	return nil
}

func newServer(cfg *config, logger *zap.Logger) (*server, error) {
	db, err := badger.Open(badger.DefaultOptions(filepath.Join(cfg.path, "data")))
	if err != nil {
		logger.Fatal("Could not open connection to badger db", zap.Error(err))
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.id)

	if err != nil {
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(cfg.path, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, err
	}
	raddr, err := net.ResolveTCPAddr("tcp", cfg.addr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.addr, raddr, 3, raftTimeout, os.Stderr)
	if err != nil {
		return nil, err
	}
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(cfg.path, "raft.db"))
	if err != nil {
		return nil, err
	}
	logStore := boltDB
	stableStore := boltDB
	fsm := raftFSM{db: db, logger: logger}
	rf, err := raft.NewRaft(raftConfig, &fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}
	if cfg.leader {
		config := raft.Configuration{Servers: []raft.Server{{
			ID:      raft.ServerID(cfg.id),
			Address: transport.LocalAddr(),
		}}}
		rf.BootstrapCluster(config)
	}
	srv := &server{
		logger: logger,
		raft:   rf,
		fsm:    &fsm,
		db:     db,
		cfg:    cfg,
	}
	return srv, nil
}

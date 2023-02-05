package standalone_storage

import (
	"io/ioutil"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db  *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dir, err := ioutil.TempDir("", "standAlone")
	if err != nil {
		log.Fatal(err)
	}
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	reader := StandAloneStorageReader{
		db: s.db,
	}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	w := new(engine_util.WriteBatch)
	for i := range batch {
		switch batch[i].Data.(type) {
		case storage.Put:
			w.SetCF(batch[i].Cf(), batch[i].Key(), batch[i].Value())
		case storage.Delete:
			w.DeleteCF(batch[i].Cf(), batch[i].Key())
		}
	}
	err := w.WriteToDB(s.db)		// 原子的事务
	if err != nil {
		log.Warn(err)
	}
	return err
}

type StandAloneStorageReader struct {
	db   *badger.DB
	txn  *badger.Txn
	iter *engine_util.BadgerIterator
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) (val []byte, err error) {
	val, _ = engine_util.GetCF(s.db, cf, key)
	return val, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	s.txn = s.db.NewTransaction(false)
	s.iter = engine_util.NewCFIterator(cf, s.txn)
	return s.iter
}

func (s *StandAloneStorageReader) Close() {
	s.iter.Close()
	s.txn.Discard()
}

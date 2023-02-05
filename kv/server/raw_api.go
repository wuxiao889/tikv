package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, _ := server.storage.Reader(nil)
	val, _ := reader.GetCF(req.Cf, req.Key)
	resp := &kvrpcpb.RawGetResponse{}
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	d := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	modify := storage.Modify{Data: d}
	err := server.storage.Write(nil, []storage.Modify{modify})
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	d := storage.Delete{Key: req.Key, Cf: req.Cf}
	m := storage.Modify{Data: d}
	err := server.storage.Write(nil, []storage.Modify{m})
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, _ := server.storage.Reader(nil)
	iter := reader.IterCF(req.Cf)
	defer reader.Close()
	kvs := make([]*kvrpcpb.KvPair, 0, req.Limit)
	limit := req.Limit
	for iter.Seek(req.StartKey); iter.Valid() && limit > 0; iter.Next() {
		limit--
		val, _ := iter.Item().ValueCopy(nil)
		kv := &kvrpcpb.KvPair{Key: iter.Item().KeyCopy(nil), Value: val}
		kvs = append(kvs, kv)
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}

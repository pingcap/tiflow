package servermaster

import (
	"context"
	"encoding/json"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// resetExecutor loads existing executor information from meta storage
// TODO: to make concurrent happens before semantic more accurate, we may introduce
// some mechanisms such as cdc etcd_worker.
func (s *Server) resetExecutor(ctx context.Context) error {
	resp, err := s.etcdClient.Get(ctx, adapter.NodeInfoKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		err := s.resetExecHandler(kv.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

// resetExecHandle unmarshals executor info and resets related information
func (s *Server) resetExecHandler(value []byte) error {
	info := &model.NodeInfo{}
	err := json.Unmarshal(value, info)
	if err != nil {
		return err
	}
	if info.Type == model.NodeTypeExecutor {
		s.executorManager.RegisterExec(info)
	}
	return nil
}

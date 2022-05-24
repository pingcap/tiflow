// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package servermaster

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
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

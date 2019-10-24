// Copyright 2019 PingCAP, Inc.
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

package storage

import (
	"context"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/roles"
	"github.com/pingcap/tidb-cdc/pkg/util"
)

type ChangeFeedInfoRWriter struct {
	etcdClient *clientv3.Client
}

func NewChangeFeedInfoRWriter(cli *clientv3.Client) *ChangeFeedInfoRWriter {
	return &ChangeFeedInfoRWriter{
		etcdClient: cli,
	}
}

func (rw *ChangeFeedInfoRWriter) Read(ctx context.Context) (map[roles.ChangeFeedID]roles.ProcessorsInfos, error) {
	_, details, err := kv.GetChangeFeedList(ctx, rw.etcdClient)
	if err != nil {
		return nil, err
	}
	result := make(map[string]roles.ProcessorsInfos, len(details))
	for changefeedID, _ := range details {
		key := kv.GetEtcdKeySubChangeFeedList(changefeedID)
		resp, err := rw.etcdClient.Get(ctx, key, clientv3.WithPrefix())
		if err != nil {
			return nil, errors.Trace(err)
		}
		pinfo := make(map[string]*roles.SubChangeFeedInfo, resp.Count)
		for _, rawKv := range resp.Kvs {
			captureID, err := util.ExtractKeySuffix(string(rawKv.Key))
			if err != nil {
				return nil, err
			}
			info, err := roles.DecodeSubChangeFeedInfo(rawKv.Value)
			if err != nil {
				return nil, err
			}
			pinfo[captureID] = info
		}
		result[changefeedID] = pinfo
	}
	return result, nil
}

func (rw *ChangeFeedInfoRWriter) Write(ctx context.Context, infos map[roles.ChangeFeedID]*roles.ChangeFeedInfo) error {
	for changefeedID, info := range infos {
		storeVal, err := info.String()
		if err != nil {
			return err
		}
		key := kv.GetEtcdKeyChangeFeedStatus(changefeedID)
		_, err = rw.etcdClient.Put(ctx, key, storeVal)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

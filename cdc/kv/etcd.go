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

package kv

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-cdc/cdc/model"
	"github.com/pingcap/tidb-cdc/pkg/util"
)

const (
	EtcdKeyBase = "/tidb/cdc"
)

// GetEtcdKeyChangeFeedList returns the prefix key of all changefeed config
func GetEtcdKeyChangeFeedList() string {
	return fmt.Sprintf("%s/changefeed/config", EtcdKeyBase)
}

// GetEtcdKeyChangeFeedConfig returns the key of a changefeed config
func GetEtcdKeyChangeFeedConfig(changefeedID string) string {
	return fmt.Sprintf("%s/%s", GetEtcdKeyChangeFeedList(), changefeedID)
}

// GetEtcdKeyChangeFeedStatus returns the key of a changefeed status
func GetEtcdKeyChangeFeedStatus(changefeedID string) string {
	return fmt.Sprintf("%s/changefeed/status/%s", EtcdKeyBase, changefeedID)
}

// GetEtcdKeyChangeFeedList returns the key of a subchangefeed info without captureID part
func GetEtcdKeySubChangeFeedList(changefeedID string) string {
	return fmt.Sprintf("%s/changefeed/subchangfeed/%s", EtcdKeyBase, changefeedID)
}

// GetEtcdKeySubChangeFeed returns the key of a subchangefeed infoformation
func GetEtcdKeySubChangeFeed(changefeedID, captureID string) string {
	return fmt.Sprintf("%s/%s", GetEtcdKeySubChangeFeedList(changefeedID), captureID)
}

// GetEtcdKeyChangeFeedList returns the prefix key of all capture info
func GetEtcdKeyCaptureList() string {
	return EtcdKeyBase + "/capture/info"
}

// GetChangeFeeds returns kv revision and a map mapping from changefeedID to changefeed detail mvccpb.KeyValue
func GetChangeFeeds(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (int64, map[string]*mvccpb.KeyValue, error) {
	key := GetEtcdKeyChangeFeedList()

	resp, err := cli.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	revision := resp.Header.Revision
	details := make(map[string]*mvccpb.KeyValue, resp.Count)
	for _, kv := range resp.Kvs {
		id, err := util.ExtractKeySuffix(string(kv.Key))
		if err != nil {
			return 0, nil, err
		}
		details[id] = kv
	}
	return revision, details, nil
}

// GetChangeFeedConfig queries the config of a given changefeed
func GetChangeFeedConfig(ctx context.Context, cli *clientv3.Client, id string, opts ...clientv3.OpOption) (*model.ChangeFeedDetail, error) {
	detail := &model.ChangeFeedDetail{}
	key := GetEtcdKeyChangeFeedConfig(id)
	resp, err := cli.Get(ctx, key, opts...)
	if err != nil {
		return detail, errors.Trace(err)
	}

	if resp.Count == 0 {
		return detail, errors.Errorf("changefeed %s not exists", id)
	}

	err = detail.Unmarshal(resp.Kvs[0].Value)
	return detail, errors.Trace(err)
}

// GetCaptures returns kv revision and CaptureInfo list
func GetCaptures(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (int64, []*model.CaptureInfo, error) {
	key := GetEtcdKeyCaptureList()

	resp, err := cli.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	revision := resp.Header.Revision
	infos := make([]*model.CaptureInfo, 0, resp.Count)
	for _, kv := range resp.Kvs {
		info := &model.CaptureInfo{}
		err := info.Unmarshal(kv.Value)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		infos = append(infos, info)
	}
	return revision, infos, nil
}

// SaveChangeFeedDetail stores change feed detail into etcd
// TODO: this should be called from outer system, such as from a TiDB client
func SaveChangeFeedDetail(ctx context.Context, client *clientv3.Client, detail *model.ChangeFeedDetail, changeFeedID string) error {
	key := GetEtcdKeyChangeFeedConfig(changeFeedID)
	value, err := detail.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = client.Put(ctx, key, value)
	return errors.Trace(err)
}

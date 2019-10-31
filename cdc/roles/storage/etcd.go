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
	"github.com/coreos/etcd/embed"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/model"
)

// ChangeFeedInfoRWriter implements `roles.ChangeFeedInfoRWriter` interface
type ChangeFeedInfoRWriter struct {
	etcdClient *clientv3.Client
}

// NewChangeFeedInfoEtcdRWriter returns a new `*ChangeFeedInfoRWriter` instance
func NewChangeFeedInfoEtcdRWriter(cli *clientv3.Client) *ChangeFeedInfoRWriter {
	return &ChangeFeedInfoRWriter{
		etcdClient: cli,
	}
}

// Read reads from etcd, and returns map mapping from changefeedID to `model.ProcessorsInfos`
func (rw *ChangeFeedInfoRWriter) Read(ctx context.Context) (map[model.ChangeFeedID]model.ProcessorsInfos, error) {
	_, details, err := kv.GetChangeFeeds(ctx, rw.etcdClient)
	if err != nil {
		return nil, err
	}
	result := make(map[string]model.ProcessorsInfos, len(details))
	for changefeedID := range details {
		pinfo, err := kv.GetSubChangeFeedInfos(ctx, rw.etcdClient, changefeedID)
		if err != nil {
			return nil, err
		}
		result[changefeedID] = pinfo
	}
	return result, nil
}

// Write writes ChangeFeedInfo of each changefeed into etcd
func (rw *ChangeFeedInfoRWriter) Write(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedInfo) error {
	var (
		txn = rw.etcdClient.KV.Txn(ctx)
		ops = make([]clientv3.Op, 0, embed.DefaultMaxTxnOps)
	)
	for changefeedID, info := range infos {
		storeVal, err := info.Marshal()
		if err != nil {
			return err
		}
		key := kv.GetEtcdKeyChangeFeedStatus(changefeedID)
		ops = append(ops, clientv3.OpPut(key, storeVal))
		if uint(len(ops)) >= embed.DefaultMaxTxnOps {
			_, err = txn.Then(ops...).Commit()
			if err != nil {
				return errors.Trace(err)
			}
			txn = rw.etcdClient.KV.Txn(ctx)
			ops = ops[:0]
		}
	}
	if len(ops) > 0 {
		_, err := txn.Then(ops...).Commit()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ProcessorTSEtcdRWriter implements `roles.ProcessorTSRWriter` interface
type ProcessorTSEtcdRWriter struct {
	etcdClient   *clientv3.Client
	changefeedID string
	captureID    string
	modRevision  int64
	info         *model.SubChangeFeedInfo
}

// NewProcessorTSEtcdRWriter returns a new `*ChangeFeedInfoRWriter` instance
func NewProcessorTSEtcdRWriter(cli *clientv3.Client, changefeedID, captureID string) *ProcessorTSEtcdRWriter {
	return &ProcessorTSEtcdRWriter{
		etcdClient:   cli,
		changefeedID: changefeedID,
		captureID:    captureID,
	}
}

func (rw *ProcessorTSEtcdRWriter) updateSubChangeFeedInfo(ctx context.Context) error {
	revision, info, err := kv.GetSubChangeFeedInfo(ctx, rw.etcdClient, rw.changefeedID, rw.captureID)
	if err != nil {
		return err
	}
	rw.modRevision = revision
	rw.info = info
	return nil
}

func (rw *ProcessorTSEtcdRWriter) writeKVWithRetry(
	ctx context.Context,
	key string,
	genValueFn func(rw *ProcessorTSEtcdRWriter, ts uint64) (string, error),
	ts uint64,
	retryCount int,
) error {
	value, err := genValueFn(rw, ts)
	if err != nil {
		return err
	}
	resp, err := rw.etcdClient.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), "=", rw.modRevision),
	).Then(
		clientv3.OpPut(key, value),
	).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		if retryCount > 0 {
			err := rw.updateSubChangeFeedInfo(ctx)
			if err != nil {
				return err
			}
			return rw.writeKVWithRetry(ctx, key, genValueFn, ts, retryCount-1)
		}
		return errors.Annotatef(model.ErrWriteTsConflict, "key: %s", key)
	}
	rw.modRevision = resp.Header.Revision
	return nil
}

// WriteResolvedTS writes the loacl resolvedTS into etcd
func (rw *ProcessorTSEtcdRWriter) WriteResolvedTS(ctx context.Context, resolvedTS uint64) error {
	key := kv.GetEtcdKeySubChangeFeed(rw.changefeedID, rw.captureID)
	if rw.modRevision == 0 {
		err := rw.updateSubChangeFeedInfo(ctx)
		if err != nil {
			return err
		}
	}
	genFn := func(rw *ProcessorTSEtcdRWriter, ts uint64) (string, error) {
		rw.info.ResolvedTS = ts
		val, err := rw.info.Marshal()
		return val, errors.Trace(err)
	}
	err := rw.writeKVWithRetry(ctx, key, genFn, resolvedTS, 3)
	return errors.Trace(err)
}

// WriteCheckpointTS writes the checkpointTS into etcd
func (rw *ProcessorTSEtcdRWriter) WriteCheckpointTS(ctx context.Context, checkpointTS uint64) error {
	key := kv.GetEtcdKeySubChangeFeed(rw.changefeedID, rw.captureID)
	if rw.modRevision == 0 {
		err := rw.updateSubChangeFeedInfo(ctx)
		if err != nil {
			return err
		}
	}
	genFn := func(rw *ProcessorTSEtcdRWriter, ts uint64) (string, error) {
		rw.info.CheckPointTS = ts
		val, err := rw.info.Marshal()
		return val, errors.Trace(err)
	}
	err := rw.writeKVWithRetry(ctx, key, genFn, checkpointTS, 3)
	return errors.Trace(err)
}

// ReadGlobalResolvedTS reads the global resolvedTS from etcd
func (rw *ProcessorTSEtcdRWriter) ReadGlobalResolvedTS(ctx context.Context) (uint64, error) {
	info, err := kv.GetChangeFeedInfo(ctx, rw.etcdClient, rw.changefeedID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return info.ResolvedTS, nil
}

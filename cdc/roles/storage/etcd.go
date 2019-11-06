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
	"sync"

	"github.com/cenkalti/backoff"
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

// ProcessorTsEtcdRWriter implements `roles.ProcessorTsRWriter` interface
type ProcessorTsEtcdRWriter struct {
	lock         sync.Mutex
	etcdClient   *clientv3.Client
	changefeedID string
	captureID    string
	modRevision  int64
	info         *model.SubChangeFeedInfo
}

// NewProcessorTsEtcdRWriter returns a new `*ChangeFeedInfoRWriter` instance
func NewProcessorTsEtcdRWriter(cli *clientv3.Client, changefeedID, captureID string) *ProcessorTsEtcdRWriter {
	return &ProcessorTsEtcdRWriter{
		etcdClient:   cli,
		changefeedID: changefeedID,
		captureID:    captureID,
	}
}

// updateSubChangeFeedInfo queries SubChangeFeedInfo from etcd and update the memory cached value.
// This function is not thread safe.
func (rw *ProcessorTsEtcdRWriter) updateSubChangeFeedInfo(ctx context.Context) error {
	revision, info, err := kv.GetSubChangeFeedInfo(ctx, rw.etcdClient, rw.changefeedID, rw.captureID)
	if err != nil {
		return err
	}
	rw.modRevision = revision
	rw.info = info
	return nil
}

// writeTsOrUpToDate updates new SubChangeFeed info into etcd if the cached info
// is up to date, otherwise update the cached SubChangeFeed info to the etcd value.
// This function is not thread safe.
func (rw *ProcessorTsEtcdRWriter) writeTsOrUpToDate(ctx context.Context) error {
	key := kv.GetEtcdKeySubChangeFeed(rw.changefeedID, rw.captureID)
	value, err := rw.info.Marshal()
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
		err2 := rw.updateSubChangeFeedInfo(ctx)
		if err2 != nil {
			return err2
		}
		return errors.Annotatef(model.ErrWriteTsConflict, "key: %s", key)
	}

	rw.modRevision = resp.Header.Revision
	return nil
}

func (rw *ProcessorTsEtcdRWriter) retryWriteTs(ctx context.Context, updateTsFn func()) error {
	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		3,
	)

	err := backoff.Retry(func() error {
		rw.lock.Lock()
		defer rw.lock.Unlock()
		updateTsFn()
		err := rw.writeTsOrUpToDate(ctx)
		if err != nil && errors.Cause(err) == context.Canceled {
			return backoff.Permanent(err)
		}
		return err

	}, retryCfg)

	return err
}

func (rw *ProcessorTsEtcdRWriter) ensureCacheData(ctx context.Context) error {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	if rw.modRevision == 0 {
		return rw.updateSubChangeFeedInfo(ctx)
	}
	return nil
}

// WriteResolvedTs writes the loacl resolvedTs into etcd
func (rw *ProcessorTsEtcdRWriter) WriteResolvedTs(ctx context.Context, resolvedTs uint64) error {
	err := rw.ensureCacheData(ctx)
	if err != nil {
		return err
	}

	return rw.retryWriteTs(ctx, func() {
		rw.info.ResolvedTs = resolvedTs
	})
}

// WriteCheckpointTs writes the checkpointTs into etcd
func (rw *ProcessorTsEtcdRWriter) WriteCheckpointTs(ctx context.Context, checkpointTs uint64) error {
	err := rw.ensureCacheData(ctx)
	if err != nil {
		return err
	}

	return rw.retryWriteTs(ctx, func() {
		rw.info.CheckPointTs = checkpointTs
	})
}

// ReadGlobalResolvedTs reads the global resolvedTs from etcd
func (rw *ProcessorTsEtcdRWriter) ReadGlobalResolvedTs(ctx context.Context) (uint64, error) {
	info, err := kv.GetChangeFeedInfo(ctx, rw.etcdClient, rw.changefeedID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return info.ResolvedTs, nil
}

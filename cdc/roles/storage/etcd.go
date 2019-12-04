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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
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

// Read reads from etcd, and returns
// - map mapping from changefeedID to `*model.ChangeFeedDetail`
// - map mapping from changefeedID to `model.ProcessorsInfos`
func (rw *ChangeFeedInfoRWriter) Read(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedDetail, map[model.ChangeFeedID]model.ProcessorsInfos, error) {
	_, details, err := kv.GetChangeFeeds(ctx, rw.etcdClient)
	if err != nil {
		return nil, nil, err
	}
	changefeeds := make(map[string]*model.ChangeFeedDetail, len(details))
	pinfos := make(map[string]model.ProcessorsInfos, len(details))
	for changefeedID, rawKv := range details {
		changefeed := &model.ChangeFeedDetail{}
		err := changefeed.Unmarshal(rawKv.Value)
		if err != nil {
			return nil, nil, err
		}

		pinfo, err := kv.GetSubChangeFeedInfos(ctx, rw.etcdClient, changefeedID)
		if err != nil {
			return nil, nil, err
		}

		// Set changefeed info if exists.
		changefeedInfo, err := kv.GetChangeFeedInfo(ctx, rw.etcdClient, changefeedID)

		switch errors.Cause(err) {
		case nil:
			changefeed.Info = changefeedInfo
		case model.ErrChangeFeedNotExists:
		default:
			return nil, nil, err
		}

		changefeeds[changefeedID] = changefeed
		pinfos[changefeedID] = pinfo
	}
	return changefeeds, pinfos, nil
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

// ProcessorTsRWriter reads or writes the resolvedTs and checkpointTs from the storage
type ProcessorTsRWriter interface {
	// ReadGlobalResolvedTs read the bloable resolved ts.
	ReadGlobalResolvedTs(ctx context.Context) (uint64, error)

	// The flowing methods *IS NOT* thread safe.
	// GetSubChangeFeedInfo returns the in memory cache *model.SubChangeFeedInfo
	GetSubChangeFeedInfo() *model.SubChangeFeedInfo
	// UpdateInfo update the in memory cache as info in storage.
	// oldInfo and newInfo is the old and new in memory cache info.
	UpdateInfo(ctx context.Context) (oldInfo *model.SubChangeFeedInfo, newInfo *model.SubChangeFeedInfo, err error)
	// WriteInfoIntoStorage update info into storage, return model.ErrWriteTsConflict if in last learn info is out dated and must call UpdateInfo.
	WriteInfoIntoStorage(ctx context.Context) error
}

var _ ProcessorTsRWriter = &ProcessorTsEtcdRWriter{}

// ProcessorTsEtcdRWriter implements `ProcessorTsRWriter` interface
type ProcessorTsEtcdRWriter struct {
	etcdClient   *clientv3.Client
	changefeedID string
	captureID    string
	modRevision  int64
	info         *model.SubChangeFeedInfo
	logger       *zap.Logger
}

// NewProcessorTsEtcdRWriter returns a new `*ChangeFeedInfoRWriter` instance
func NewProcessorTsEtcdRWriter(cli *clientv3.Client, changefeedID, captureID string) (*ProcessorTsEtcdRWriter, error) {
	logger := log.L().With(zap.String("changefeed id", changefeedID)).
		With(zap.String("capture id", captureID))

	rw := &ProcessorTsEtcdRWriter{
		etcdClient:   cli,
		changefeedID: changefeedID,
		captureID:    captureID,
		info:         &model.SubChangeFeedInfo{},
		logger:       logger,
	}

	var err error
	rw.modRevision, rw.info, err = kv.GetSubChangeFeedInfo(context.Background(), rw.etcdClient, rw.changefeedID, rw.captureID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return rw, nil
}

// UpdateInfo implements ProcessorTsRWriter interface.
func (rw *ProcessorTsEtcdRWriter) UpdateInfo(
	ctx context.Context,
) (oldInfo *model.SubChangeFeedInfo, newInfo *model.SubChangeFeedInfo, err error) {
	modRevision, info, err := kv.GetSubChangeFeedInfo(ctx, rw.etcdClient, rw.changefeedID, rw.captureID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	oldInfo = rw.info
	newInfo = info
	rw.info = newInfo
	rw.modRevision = modRevision
	return
}

// WriteInfoIntoStorage write info into storage, return model.ErrWriteTsConflict if the latest info is outdated.
func (rw *ProcessorTsEtcdRWriter) WriteInfoIntoStorage(
	ctx context.Context,
) error {
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
		log.Info("outdated table infos, ignore update info")
		return errors.Annotatef(model.ErrWriteTsConflict, "key: %s", key)
	}

	rw.logger.Debug("update subchangefeed info success",
		zap.Int64("modRevision", rw.modRevision),
		zap.Stringer("info", rw.info))

	rw.modRevision = resp.Header.Revision
	return nil
}

// ReadGlobalResolvedTs reads the global resolvedTs from etcd
func (rw *ProcessorTsEtcdRWriter) ReadGlobalResolvedTs(ctx context.Context) (uint64, error) {
	info, err := kv.GetChangeFeedInfo(ctx, rw.etcdClient, rw.changefeedID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return info.ResolvedTs, nil
}

// GetSubChangeFeedInfo returns the in memory cache of *model.SubChangeFeedInfo stored in ProcessorTsEtcdRWriter
func (rw *ProcessorTsEtcdRWriter) GetSubChangeFeedInfo() *model.SubChangeFeedInfo {
	return rw.info
}

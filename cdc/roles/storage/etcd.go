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
	"time"

	"github.com/cenkalti/backoff"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
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

// ProcessorTsEtcdRWriter implements `roles.ProcessorTsRWriter` interface
type ProcessorTsEtcdRWriter struct {
	lock         sync.Mutex
	etcdClient   *clientv3.Client
	changefeedID string
	captureID    string
	modRevision  int64
	info         *model.SubChangeFeedInfo
	logger       *zap.Logger
}

// NewProcessorTsEtcdRWriter returns a new `*ChangeFeedInfoRWriter` instance
func NewProcessorTsEtcdRWriter(cli *clientv3.Client, changefeedID, captureID string) *ProcessorTsEtcdRWriter {
	logger := log.L().With(zap.String("changefeed id", changefeedID)).
		With(zap.String("capture id", captureID))

	return &ProcessorTsEtcdRWriter{
		etcdClient:   cli,
		changefeedID: changefeedID,
		captureID:    captureID,
		info:         &model.SubChangeFeedInfo{},
		logger:       logger,
	}
}

// updateSubChangeFeedInfo queries SubChangeFeedInfo from etcd and update the memory cached value.
// This function is not thread safe.
func (rw *ProcessorTsEtcdRWriter) updateSubChangeFeedInfo(
	ctx context.Context,
	updateInfoFn func(info *model.SubChangeFeedInfo),
) error {
	modRevision, info, err := kv.GetSubChangeFeedInfo(ctx, rw.etcdClient, rw.changefeedID, rw.captureID)
	if err != nil {
		return err
	}
	rw.modRevision = modRevision
	updateInfoFn(info)
	rw.logger.Debug("update info from etcd", zap.Stringer("info", info))
	return nil
}

// updateFullInfo updates rw.info to given info. This function is not thread safe.
func (rw *ProcessorTsEtcdRWriter) updateFullInfo(info *model.SubChangeFeedInfo) {
	rw.info = info
}

// updatePartialInfo only updates checkpointTs and resolvedTs of rw.info. This function is not thread safe.
func (rw *ProcessorTsEtcdRWriter) updatePartialInfo(info *model.SubChangeFeedInfo) {
	rw.info.CheckPointTs = info.CheckPointTs
	rw.info.ResolvedTs = info.ResolvedTs
}

// writeTsOrUpToDate updates new SubChangeFeed info into etcd if the cached info
// is up to date, otherwise update the cached SubChangeFeed info to the etcd value
// via `updateInfoFn` function. This function is not thread safe.
func (rw *ProcessorTsEtcdRWriter) writeTsOrUpToDate(
	ctx context.Context,
	updateInfoFn func(*model.SubChangeFeedInfo),
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
		err2 := rw.updateSubChangeFeedInfo(ctx, updateInfoFn)
		if err2 != nil {
			return err2
		}

		log.Info("outdated table infos, ignore update timestamp")
		return errors.Annotatef(model.ErrWriteTsConflict, "key: %s", key)
	}

	rw.logger.Debug("update subchangefeed info success",
		zap.Int64("modRevision", rw.modRevision),
		zap.Stringer("info", rw.info))

	rw.modRevision = resp.Header.Revision
	return nil
}

// retryWriteData writes `SubChangeFeedInfo` data into etcd
// `updateLocalDataFn` is used to update local cached `SubChangeFeedInfo`
// `updateInfoFn` is used to update local cached `SubChangeFeedInfo` with remote storage value
func (rw *ProcessorTsEtcdRWriter) retryWriteData(
	ctx context.Context,
	updateLocalDataFn func(),
	updateInfoFn func(*model.SubChangeFeedInfo),
) error {
	err := retry.Run(func() error {
		rw.lock.Lock()
		defer rw.lock.Unlock()
		updateLocalDataFn()
		return rw.writeTsOrUpToDate(ctx, updateInfoFn)
	}, 3)
	return err
}

func (rw *ProcessorTsEtcdRWriter) ensureCacheData(ctx context.Context) error {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	if rw.modRevision == 0 {
		return rw.updateSubChangeFeedInfo(ctx, rw.updateFullInfo)
	}
	return nil
}

// WriteResolvedTs writes the loacl resolvedTs into etcd, and updates
// checkpointTs and resolvedTs of `Info` in local cache if needed.
// NOTE the TableInfos, TablePLock and TableCLock are not updated even they have
// been changed in etcd.
func (rw *ProcessorTsEtcdRWriter) WriteResolvedTs(ctx context.Context, resolvedTs uint64) error {
	err := rw.ensureCacheData(ctx)
	if err != nil {
		return err
	}

	return rw.retryWriteData(ctx, func() { rw.info.ResolvedTs = resolvedTs }, rw.updatePartialInfo)
}

// WriteCheckpointTs writes the checkpointTs into etcd
func (rw *ProcessorTsEtcdRWriter) WriteCheckpointTs(ctx context.Context, checkpointTs uint64) error {
	err := rw.ensureCacheData(ctx)
	if err != nil {
		return err
	}

	return rw.retryWriteData(ctx, func() { rw.info.CheckPointTs = checkpointTs }, rw.updateFullInfo)
}

// ReadGlobalResolvedTs reads the global resolvedTs from etcd
func (rw *ProcessorTsEtcdRWriter) ReadGlobalResolvedTs(ctx context.Context) (uint64, error) {
	info, err := kv.GetChangeFeedInfo(ctx, rw.etcdClient, rw.changefeedID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return info.ResolvedTs, nil
}

// GetSubChangeFeedInfo returns a deep copy of *model.SubChangeFeedInfo stored in ProcessorTsEtcdRWriter
func (rw *ProcessorTsEtcdRWriter) GetSubChangeFeedInfo() *model.SubChangeFeedInfo {
	return rw.info.Clone()
}

// WriteTableCLock writes C-lock to etcd
func (rw *ProcessorTsEtcdRWriter) WriteTableCLock(ctx context.Context, checkpointTs uint64) error {
	lock := &model.TableLock{
		Ts:           rw.info.TablePLock.Ts,
		CheckpointTs: checkpointTs,
	}
	// when P-lock is not paired, owner can't update TableInfos in SubChangeFeedInfo
	return rw.retryWriteData(ctx, func() { rw.info.TableCLock = lock }, rw.updateFullInfo)
}

// OwnerSubCFInfoEtcdWriter encapsulates SubChangeFeedInfo write operation
type OwnerSubCFInfoEtcdWriter struct {
	etcdClient *clientv3.Client
}

// NewOwnerSubCFInfoEtcdWriter returns a new `*OwnerSubCFInfoEtcdWriter` instance
func NewOwnerSubCFInfoEtcdWriter(cli *clientv3.Client) *OwnerSubCFInfoEtcdWriter {
	return &OwnerSubCFInfoEtcdWriter{
		etcdClient: cli,
	}
}

// updateInfo updates the local SubChangeFeedInfo with etcd value, except for TableInfos and TablePLock
func (ow *OwnerSubCFInfoEtcdWriter) updateInfo(
	ctx context.Context, changefeedID, captureID string, oldInfo *model.SubChangeFeedInfo,
) (newInfo *model.SubChangeFeedInfo, err error) {
	modRevision, info, err := kv.GetSubChangeFeedInfo(ctx, ow.etcdClient, changefeedID, captureID)
	if err != nil {
		return
	}

	// TableInfos and TablePLock is updated by owner only
	tableInfos := oldInfo.TableInfos
	pLock := oldInfo.TablePLock
	newInfo = info
	newInfo.TableInfos = tableInfos
	newInfo.TablePLock = pLock
	newInfo.ModRevision = modRevision

	if newInfo.TablePLock != nil {
		if newInfo.TableCLock == nil {
			err = model.ErrFindPLockNotCommit
		} else {
			// clean lock
			newInfo.TablePLock = nil
			newInfo.TableCLock = nil
		}
	}
	return
}

// checkLock checks whether there exists p-lock or whether p-lock is committed if it exists
func (ow *OwnerSubCFInfoEtcdWriter) checkLock(
	ctx context.Context, changefeedID, captureID string,
) (status model.TableLockStatus, err error) {
	_, info, err := kv.GetSubChangeFeedInfo(ctx, ow.etcdClient, changefeedID, captureID)
	if err != nil {
		if errors.Cause(err) == model.ErrSubChangeFeedInfoNotExists {
			return model.TableNoLock, nil
		}
		return
	}

	// in most cases there is no p-lock
	if info.TablePLock == nil {
		status = model.TableNoLock
		return
	}

	if info.TableCLock != nil {
		status = model.TablePLockCommited
	} else {
		status = model.TablePLock
	}

	return
}

// Write persists given `SubChangeFeedInfo` into etcd
func (ow *OwnerSubCFInfoEtcdWriter) Write(
	ctx context.Context,
	changefeedID, captureID string,
	info *model.SubChangeFeedInfo,
	writePLock bool,
) (newInfo *model.SubChangeFeedInfo, err error) {

	// check p-lock not exists or is already resolved
	lockStatus, err := ow.checkLock(ctx, changefeedID, captureID)
	if err != nil {
		return
	}
	newInfo = info
	switch lockStatus {
	case model.TableNoLock:
	case model.TablePLockCommited:
		newInfo.TablePLock = nil
		newInfo.TableCLock = nil
	case model.TablePLock:
		err = errors.Trace(model.ErrFindPLockNotCommit)
		return
	}

	if writePLock {
		newInfo.TablePLock = &model.TableLock{
			Ts:        oracle.EncodeTSO(time.Now().UnixNano() / int64(time.Millisecond)),
			CreatorID: util.CaptureIDFromCtx(ctx),
		}
	}

	key := kv.GetEtcdKeySubChangeFeed(changefeedID, captureID)
	err = retry.Run(func() error {
		value, err := newInfo.Marshal()
		if err != nil {
			return err
		}

		resp, err := ow.etcdClient.KV.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(key), "=", newInfo.ModRevision),
		).Then(
			clientv3.OpPut(key, value),
		).Commit()

		if err != nil {
			return errors.Trace(err)
		}

		if !resp.Succeeded {
			log.Info("outdated table infos, update table and retry")
			newInfo, err = ow.updateInfo(ctx, changefeedID, captureID, info)
			switch errors.Cause(err) {
			case model.ErrFindPLockNotCommit:
				return backoff.Permanent(err)
			case nil:
				return model.ErrWriteSubChangeFeedInfoConlict
			default:
				return err
			}
		}

		newInfo.ModRevision = resp.Header.Revision

		return nil
	}, 5)

	return
}

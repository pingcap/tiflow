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

// ChangeFeedRWriter implements `roles.ChangeFeedRWriter` interface
type ChangeFeedRWriter struct {
	etcdClient *clientv3.Client
}

// NewChangeFeedEtcdRWriter returns a new `*ChangeFeedRWriter` instance
func NewChangeFeedEtcdRWriter(cli *clientv3.Client) *ChangeFeedRWriter {
	return &ChangeFeedRWriter{
		etcdClient: cli,
	}
}

// Read reads from etcd, and returns
// - map mapping from changefeedID to `*model.ChangeFeedInfo`
// - map mapping from changefeedID to `model.ProcessorsInfos`
func (rw *ChangeFeedRWriter) Read(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedInfo, map[model.ChangeFeedID]*model.ChangeFeedStatus, map[model.ChangeFeedID]model.ProcessorsInfos, error) {
	_, details, err := kv.GetChangeFeeds(ctx, rw.etcdClient)
	if err != nil {
		return nil, nil, nil, err
	}
	changefeedInfos := make(map[string]*model.ChangeFeedInfo, len(details))
	changefeedStatus := make(map[string]*model.ChangeFeedStatus, len(details))
	pinfos := make(map[string]model.ProcessorsInfos, len(details))
	for changefeedID, rawKv := range details {
		changefeed := &model.ChangeFeedInfo{}
		err := changefeed.Unmarshal(rawKv.Value)
		if err != nil {
			return nil, nil, nil, err
		}

		pinfo, err := kv.GetAllTaskStatus(ctx, rw.etcdClient, changefeedID)
		if err != nil {
			return nil, nil, nil, err
		}

		status, err := kv.GetChangeFeedStatus(ctx, rw.etcdClient, changefeedID)
		if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
			return nil, nil, nil, err
		}

		changefeedStatus[changefeedID] = status
		changefeedInfos[changefeedID] = changefeed
		pinfos[changefeedID] = pinfo
	}
	return changefeedInfos, changefeedStatus, pinfos, nil
}

// Write writes ChangeFeedStatus of each changefeed into etcd
func (rw *ChangeFeedRWriter) Write(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedStatus) error {
	var (
		txn = rw.etcdClient.KV.Txn(ctx)
		ops = make([]clientv3.Op, 0, embed.DefaultMaxTxnOps)
	)
	for changefeedID, info := range infos {
		storeVal, err := info.Marshal()
		if err != nil {
			return errors.Trace(err)
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
	// GetTaskStatus returns the in memory cache *model.TaskStatus
	GetTaskStatus() *model.TaskStatus
	// UpdateInfo update the in memory cache as taskStatus in storage.
	// oldInfo and newInfo is the old and new in memory cache taskStatus.
	UpdateInfo(ctx context.Context) (oldInfo *model.TaskStatus, newInfo *model.TaskStatus, err error)
	// WriteInfoIntoStorage update taskStatus into storage, return model.ErrWriteTsConflict if in last learn taskStatus is out dated and must call UpdateInfo.
	WriteInfoIntoStorage(ctx context.Context) error
}

var _ ProcessorTsRWriter = &ProcessorTsEtcdRWriter{}

// ProcessorTsEtcdRWriter implements `ProcessorTsRWriter` interface
type ProcessorTsEtcdRWriter struct {
	etcdClient   *clientv3.Client
	changefeedID string
	captureID    string
	modRevision  int64
	taskStatus   *model.TaskStatus
	logger       *zap.Logger
}

// NewProcessorTsEtcdRWriter returns a new `*ChangeFeedRWriter` instance
func NewProcessorTsEtcdRWriter(cli *clientv3.Client, changefeedID, captureID string) (*ProcessorTsEtcdRWriter, error) {
	logger := log.L().With(zap.String("changefeed id", changefeedID)).
		With(zap.String("capture id", captureID))

	rw := &ProcessorTsEtcdRWriter{
		etcdClient:   cli,
		changefeedID: changefeedID,
		captureID:    captureID,
		taskStatus:   &model.TaskStatus{},
		logger:       logger,
	}

	var err error
	rw.modRevision, rw.taskStatus, err = kv.GetTaskStatus(context.Background(), rw.etcdClient, rw.changefeedID, rw.captureID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return rw, nil
}

// UpdateInfo implements ProcessorTsRWriter interface.
func (rw *ProcessorTsEtcdRWriter) UpdateInfo(
	ctx context.Context,
) (oldInfo *model.TaskStatus, newInfo *model.TaskStatus, err error) {
	modRevision, info, err := kv.GetTaskStatus(ctx, rw.etcdClient, rw.changefeedID, rw.captureID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	oldInfo = rw.taskStatus
	newInfo = info
	rw.taskStatus = newInfo
	rw.modRevision = modRevision
	return
}

// WriteInfoIntoStorage write taskStatus into storage, return model.ErrWriteTsConflict if the latest taskStatus is outdated.
func (rw *ProcessorTsEtcdRWriter) WriteInfoIntoStorage(
	ctx context.Context,
) error {
	key := kv.GetEtcdKeyTask(rw.changefeedID, rw.captureID)
	value, err := rw.taskStatus.Marshal()
	if err != nil {
		return errors.Trace(err)
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
		log.Info("outdated table infos, ignore update taskStatus")
		return errors.Annotatef(model.ErrWriteTsConflict, "key: %s", key)
	}

	rw.logger.Debug("update task status success",
		zap.Int64("modRevision", rw.modRevision),
		zap.Stringer("status", rw.taskStatus))

	rw.modRevision = resp.Header.Revision
	return nil
}

// ReadGlobalResolvedTs reads the global resolvedTs from etcd
func (rw *ProcessorTsEtcdRWriter) ReadGlobalResolvedTs(ctx context.Context) (uint64, error) {
	info, err := kv.GetChangeFeedStatus(ctx, rw.etcdClient, rw.changefeedID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return info.ResolvedTs, nil
}

// GetTaskStatus returns the in memory cache of *model.TaskStatus stored in ProcessorTsEtcdRWriter
func (rw *ProcessorTsEtcdRWriter) GetTaskStatus() *model.TaskStatus {
	return rw.taskStatus
}

// OwnerTaskStatusEtcdWriter encapsulates TaskStatus write operation
type OwnerTaskStatusEtcdWriter struct {
	etcdClient *clientv3.Client
}

// NewOwnerTaskStatusEtcdWriter returns a new `*OwnerTaskStatusEtcdWriter` instance
func NewOwnerTaskStatusEtcdWriter(cli *clientv3.Client) *OwnerTaskStatusEtcdWriter {
	return &OwnerTaskStatusEtcdWriter{
		etcdClient: cli,
	}
}

// updateInfo updates the local TaskStatus with etcd value, except for TableInfos, Admin and TablePLock
func (ow *OwnerTaskStatusEtcdWriter) updateInfo(
	ctx context.Context, changefeedID, captureID string, oldInfo *model.TaskStatus,
) (newInfo *model.TaskStatus, err error) {
	modRevision, info, err := kv.GetTaskStatus(ctx, ow.etcdClient, changefeedID, captureID)
	if err != nil {
		return
	}

	// TableInfos and TablePLock is updated by owner only
	newInfo = info
	newInfo.TableInfos = oldInfo.TableInfos
	newInfo.AdminJobType = oldInfo.AdminJobType
	newInfo.TablePLock = oldInfo.TablePLock
	newInfo.ModRevision = modRevision

	if newInfo.TablePLock != nil {
		if newInfo.TableCLock == nil {
			err = errors.Trace(model.ErrFindPLockNotCommit)
		} else {
			// clean lock
			newInfo.TablePLock = nil
			newInfo.TableCLock = nil
		}
	}
	return
}

// checkLock checks whether there exists p-lock or whether p-lock is committed if it exists
func (ow *OwnerTaskStatusEtcdWriter) checkLock(
	ctx context.Context, changefeedID, captureID string,
) (status model.TableLockStatus, err error) {
	_, info, err := kv.GetTaskStatus(ctx, ow.etcdClient, changefeedID, captureID)
	if err != nil {
		if errors.Cause(err) == model.ErrTaskStatusNotExists {
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

// Write persists given `TaskStatus` into etcd.
// If returned err is not nil, don't use the returned newInfo as it may be not a reasonable value.
func (ow *OwnerTaskStatusEtcdWriter) Write(
	ctx context.Context,
	changefeedID, captureID string,
	info *model.TaskStatus,
	writePLock bool,
) (newInfo *model.TaskStatus, err error) {

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

	key := kv.GetEtcdKeyTask(changefeedID, captureID)
	err = retry.Run(func() error {
		value, err := newInfo.Marshal()
		if err != nil {
			return errors.Trace(err)
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
				return errors.Trace(model.ErrWriteTaskStatusConlict)
			default:
				return errors.Trace(err)
			}
		}

		newInfo.ModRevision = resp.Header.Revision

		return nil
	}, 5)

	return
}

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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// ProcessorTsRWriter reads or writes the resolvedTs and checkpointTs from the storage
type ProcessorTsRWriter interface {
	// GetChangeFeedStatus read the changefeed status.
	GetChangeFeedStatus(ctx context.Context) (*model.ChangeFeedStatus, error)

	// WritePosition update taskPosition into storage, return model.ErrWriteTsConflict if in last learn taskPosition is out dated and must call UpdateInfo.
	WritePosition(ctx context.Context, taskPosition *model.TaskPosition) error

	// The flowing methods *IS NOT* thread safe.
	// GetTaskStatus returns the in memory cache *model.TaskStatus
	GetTaskStatus() *model.TaskStatus
	// UpdateInfo update the in memory cache as taskStatus in storage.
	// oldInfo and newInfo is the old and new in memory cache taskStatus.
	UpdateInfo(ctx context.Context) (bool, error)
	// WriteInfoIntoStorage update taskStatus into storage, return model.ErrWriteTsConflict if in last learn taskStatus is out dated and must call UpdateInfo.
	WriteInfoIntoStorage(ctx context.Context) error
}

var _ ProcessorTsRWriter = &ProcessorTsEtcdRWriter{}

// ProcessorTsEtcdRWriter implements `ProcessorTsRWriter` interface
type ProcessorTsEtcdRWriter struct {
	etcdClient   kv.CDCEtcdClient
	changefeedID string
	captureID    string
	modRevision  int64
	taskStatus   *model.TaskStatus
	logger       *zap.Logger
}

// NewProcessorTsEtcdRWriter returns a new `*ChangeFeedRWriter` instance
func NewProcessorTsEtcdRWriter(cli kv.CDCEtcdClient, changefeedID, captureID string) (*ProcessorTsEtcdRWriter, error) {
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
	rw.modRevision, rw.taskStatus, err = cli.GetTaskStatus(context.Background(), rw.changefeedID, rw.captureID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return rw, nil
}

// WritePosition implements ProcessorTsRWriter interface.
func (rw *ProcessorTsEtcdRWriter) WritePosition(ctx context.Context, taskPosition *model.TaskPosition) error {
	return errors.Trace(rw.etcdClient.PutTaskPosition(ctx, rw.changefeedID, rw.captureID, taskPosition))
}

// UpdateInfo implements ProcessorTsRWriter interface.
func (rw *ProcessorTsEtcdRWriter) UpdateInfo(
	ctx context.Context,
) (changed bool, err error) {
	modRevision, info, err := rw.etcdClient.GetTaskStatus(ctx, rw.changefeedID, rw.captureID)
	if err != nil {
		return false, errors.Trace(err)
	}
	changed = rw.modRevision != modRevision
	if changed {
		rw.taskStatus = info
		rw.modRevision = modRevision
	}
	return
}

// WriteInfoIntoStorage write taskStatus into storage, return model.ErrWriteTsConflict if the latest taskStatus is outdated.
func (rw *ProcessorTsEtcdRWriter) WriteInfoIntoStorage(
	ctx context.Context,
) error {

	key := kv.GetEtcdKeyTaskStatus(rw.changefeedID, rw.captureID)
	value, err := rw.taskStatus.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := rw.etcdClient.Client.KV.Txn(ctx).If(
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

// GetChangeFeedStatus reads the changefeed status from etcd
func (rw *ProcessorTsEtcdRWriter) GetChangeFeedStatus(ctx context.Context) (*model.ChangeFeedStatus, error) {
	return rw.etcdClient.GetChangeFeedStatus(ctx, rw.changefeedID)
}

// GetTaskStatus returns the in memory cache of *model.TaskStatus stored in ProcessorTsEtcdRWriter
func (rw *ProcessorTsEtcdRWriter) GetTaskStatus() *model.TaskStatus {
	return rw.taskStatus
}

// OwnerTaskStatusEtcdWriter encapsulates TaskStatus write operation
type OwnerTaskStatusEtcdWriter struct {
	etcdClient kv.CDCEtcdClient
}

// NewOwnerTaskStatusEtcdWriter returns a new `*OwnerTaskStatusEtcdWriter` instance
func NewOwnerTaskStatusEtcdWriter(cli kv.CDCEtcdClient) *OwnerTaskStatusEtcdWriter {
	return &OwnerTaskStatusEtcdWriter{
		etcdClient: cli,
	}
}

// updateInfo updates the local TaskStatus with etcd value, except for TableInfos, Admin and TablePLock
func (ow *OwnerTaskStatusEtcdWriter) updateInfo(
	ctx context.Context, changefeedID, captureID string, oldInfo *model.TaskStatus,
) (newInfo *model.TaskStatus, err error) {
	modRevision, info, err := ow.etcdClient.GetTaskStatus(ctx, changefeedID, captureID)
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
	_, info, err := ow.etcdClient.GetTaskStatus(ctx, changefeedID, captureID)
	if err != nil {
		if errors.Cause(err) == model.ErrTaskStatusNotExists {
			return model.TableNoLock, nil
		}
		return
	}
	log.Info("show check lock", zap.Reflect("status", info))

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

	key := kv.GetEtcdKeyTaskStatus(changefeedID, captureID)
	err = retry.Run(500*time.Millisecond, 5, func() error {
		value, err := newInfo.Marshal()
		if err != nil {
			return errors.Trace(err)
		}

		resp, err := ow.etcdClient.Client.KV.Txn(ctx).If(
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
	})

	return
}

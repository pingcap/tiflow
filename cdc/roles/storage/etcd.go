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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// ProcessorTsRWriter reads or writes the resolvedTs and checkpointTs from the storage
type ProcessorTsRWriter interface {
	// GetChangeFeedStatus read the changefeed status.
	GetChangeFeedStatus(ctx context.Context) (*model.ChangeFeedStatus, int64, error)

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
	log.Info("update taskStatus", zap.Stringer("status", rw.taskStatus))

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
func (rw *ProcessorTsEtcdRWriter) GetChangeFeedStatus(ctx context.Context) (*model.ChangeFeedStatus, int64, error) {
	return rw.etcdClient.GetChangeFeedStatus(ctx, rw.changefeedID)
}

// GetTaskStatus returns the in memory cache of *model.TaskStatus stored in ProcessorTsEtcdRWriter
func (rw *ProcessorTsEtcdRWriter) GetTaskStatus() *model.TaskStatus {
	return rw.taskStatus
}

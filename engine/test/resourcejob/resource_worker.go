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

package resourcejob

import (
	"context"
	"encoding/json"
	gerrors "errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

type workerConfig struct {
	ResourceID    resModel.ResourceID `json:"resource_id"`
	ResourceState resourceState       `json:"resource_state"`
	ResourceLen   int                 `json:"resource_len"`
}

// Worker is the worker for Resource-Job.
type Worker struct {
	framework.BaseWorker

	status *workerStatus
	config *workerConfig

	logger log.Logger
}

// NewWorker creates a new Worker.
func NewWorker(
	ctx *dcontext.Context,
	workerID frameModel.WorkerID, masterID frameModel.MasterID,
	config *workerConfig,
) framework.WorkerImpl {
	status := &workerStatus{}

	logger := log.L().WithFields(
		zap.String("job-id", masterID),
		zap.String("worker-id", workerID))

	switch config.ResourceState {
	case resourceStateUninit:
		status.State = workerStateGenerating
	case resourceStateUnsorted:
		status.State = workerStateSorting
	case resourceStateCommitting:
		status.State = workerStateCommitting
	case resourceStateSorted:
		status.State = workerStateFinished
	default:
		logger.Warn("Unexpected worker config",
			zap.Any("config", config))
		panic("unreachable")
	}

	return &Worker{
		status: status,
		config: config,
		logger: logger,
	}
}

// InitImpl implements WorkerImpl.
func (w *Worker) InitImpl(ctx context.Context) error {
	return w.persistWorkerStatus(ctx)
}

// Tick implements WorkerImpl.
func (w *Worker) Tick(ctx context.Context) error {
	switch w.status.State {
	case workerStateGenerating:
		if err := w.generateFile(ctx); err != nil {
			return err
		}
		w.status.State = workerStateSorting
	case workerStateSorting:
		if err := w.sortFile(ctx); err != nil {
			return err
		}
		w.status.State = workerStateCommitting
	case workerStateCommitting:
		if err := w.commit(ctx); err != nil {
			return err
		}
		w.status.State = workerStateFinished
	case workerStateFinished:
		return w.Exit(ctx, frameModel.WorkerStatus{
			Code: frameModel.WorkerStatusFinished,
		}, nil)
	}

	return w.persistWorkerStatus(ctx)
}

func (w *Worker) generateFile(ctx context.Context) error {
	resourceID := w.config.ResourceID
	startTime := time.Now()
	w.logger.Info("start generating file",
		zap.String("resource-id", resourceID))

	handle, err := w.OpenStorage(ctx, resourceID)
	if err != nil {
		return err
	}

	fileWriter, err := newFileWriter(ctx, handle.BrExternalStorage(), "original.txt")
	if err != nil {
		return err
	}

	for i := 0; i < w.config.ResourceLen; i++ {
		err := fileWriter.AppendInt64(ctx, rand.Int63())
		if err != nil {
			return err
		}
	}

	if err := fileWriter.Close(ctx); err != nil {
		return err
	}

	if err := handle.Persist(ctx); err != nil {
		return err
	}

	duration := time.Since(startTime)
	w.logger.Info("Finished generating file",
		zap.String("resource-id", resourceID),
		zap.Duration("duration", duration))
	return nil
}

func (w *Worker) sortFile(ctx context.Context) error {
	resourceID := w.config.ResourceID
	startTime := time.Now()
	w.logger.Info("start sorting file",
		zap.String("resource-id", resourceID))

	originalHandle, err := w.OpenStorage(ctx, resourceID)
	if err != nil {
		return err
	}

	fileReader, err := newFileReader(ctx, originalHandle.BrExternalStorage(), "original.txt")
	if err != nil {
		return err
	}
	defer func() {
		_ = fileReader.Close()
	}()

	var nums []int64
	for {
		num, err := fileReader.ReadInt64(ctx)
		if err != nil && gerrors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		nums = append(nums, num)
	}

	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})

	tempResourceID := fmt.Sprintf("%s-temp", resourceID)
	targetHandle, err := w.OpenStorage(ctx, tempResourceID)
	if err != nil {
		return err
	}

	fileWriter, err := newFileWriter(ctx, targetHandle.BrExternalStorage(), "temp.txt")
	if err != nil {
		return err
	}
	defer func() {
		_ = fileWriter.Close(ctx)
	}()

	for _, num := range nums {
		if err := fileWriter.AppendInt64(ctx, num); err != nil {
			return err
		}
	}

	if err := w.copyBack(ctx, targetHandle, originalHandle); err != nil {
		return err
	}

	duration := time.Since(startTime)
	w.logger.Info("Finished sorting file",
		zap.String("resource-id", resourceID),
		zap.Duration("duration", duration))

	return nil
}

func (w *Worker) copyBack(ctx context.Context, source, target broker.Handle) error {
	reader, err := newFileReader(ctx, source.BrExternalStorage(), "temp.txt")
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.Close()
	}()

	writer, err := newFileWriter(ctx, target.BrExternalStorage(), "final.txt")
	if err != nil {
		return err
	}
	defer func() {
		_ = writer.Close(ctx)
	}()

	for {
		num, err := reader.ReadInt64(ctx)
		if err != nil && gerrors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		if err := writer.AppendInt64(ctx, num); err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) commit(ctx context.Context) error {
	handle, err := w.OpenStorage(ctx, w.config.ResourceID)
	if err != nil {
		return err
	}

	exists, err := handle.BrExternalStorage().FileExists(ctx, "original.txt")
	if err != nil {
		return err
	}

	if exists {
		err := handle.BrExternalStorage().DeleteFile(ctx, "original.txt")
		if err != nil {
			return err
		}
	}

	return handle.BrExternalStorage().
		Rename(ctx, "final.txt", "original.txt")
}

func (w *Worker) persistWorkerStatus(ctx context.Context) error {
	rawBytes, err := json.Marshal(w.status)
	if err != nil {
		return errors.Trace(err)
	}

	status := frameModel.WorkerStatus{
		Code:     frameModel.WorkerStatusNormal,
		ExtBytes: rawBytes,
	}

	return w.UpdateStatus(ctx, status)
}

// OnMasterMessage implements WorkerImpl
func (w *Worker) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	return nil
}

// CloseImpl implements WorkerImpl.
func (w *Worker) CloseImpl(ctx context.Context) error {
	return nil
}

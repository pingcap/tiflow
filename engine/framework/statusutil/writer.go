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

package statusutil

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework/internal/worker"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Writer is used to persist WorkerStatus changes and send notifications
// to the Master.
type Writer struct {
	metaclient    pkgOrm.Client
	messageSender p2p.MessageSender
	lastStatus    frameModel.WorkerStatus

	workerID   frameModel.WorkerID
	masterInfo worker.MasterInfoProvider
}

// NewWriter creates a new Writer.
func NewWriter(
	metaclient pkgOrm.Client,
	messageSender p2p.MessageSender,
	masterInfo worker.MasterInfoProvider,
	workerID frameModel.WorkerID,
) *Writer {
	return &Writer{
		metaclient:    metaclient,
		messageSender: messageSender,
		masterInfo:    masterInfo,
		workerID:      workerID,
	}
}

// UpdateStatus checks if newStatus.HasSignificantChange() is true, if so, it persists the change and
// tries to send a notification. Note that sending the notification is asynchronous.
func (w *Writer) UpdateStatus(ctx context.Context, newStatus *frameModel.WorkerStatus) (retErr error) {
	defer func() {
		if retErr == nil {
			return
		}
		log.Warn("UpdateStatus failed",
			zap.String("worker-id", w.workerID),
			zap.String("master-id", w.masterInfo.MasterID()),
			zap.String("master-node", w.masterInfo.MasterNode()),
			zap.Int64("master-epoch", w.masterInfo.Epoch()),
			zap.Error(retErr))
	}()

	if w.lastStatus.HasSignificantChange(newStatus) {
		// Status has changed, so we need to persist the status.
		if err := w.persistStatus(ctx, newStatus); err != nil {
			return err
		}
	}

	w.lastStatus = *newStatus

	// TODO replace the timeout with a variable.
	return w.sendStatusMessageWithRetry(ctx, 15*time.Second, newStatus)
}

func (w *Writer) sendStatusMessageWithRetry(
	ctx context.Context, timeout time.Duration, newStatus *frameModel.WorkerStatus,
) error {
	// NOTE we need this function especially to handle the situation where
	// the p2p connection to the target executor is not established yet.
	// We might need one or two retries when our executor has just started up.

	retryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rl := rate.NewLimiter(rate.Every(100*time.Millisecond), 1)
	for {
		select {
		case <-retryCtx.Done():
			return errors.Trace(retryCtx.Err())
		default:
		}

		if err := rl.Wait(retryCtx); err != nil {
			return errors.Trace(err)
		}

		topic := WorkerStatusTopic(w.masterInfo.MasterID())
		// NOTE: We must read the MasterNode() in each retry in case the master is failed over.
		err := w.messageSender.SendToNodeB(ctx, w.masterInfo.MasterNode(), topic, &WorkerStatusMessage{
			Worker:      w.workerID,
			MasterEpoch: w.masterInfo.Epoch(),
			Status:      newStatus,
		})
		if err != nil {
			if errors.Is(err, errors.ErrExecutorNotFoundForMessage) {
				if err := w.masterInfo.SyncRefreshMasterInfo(ctx); err != nil {
					log.Warn("failed to refresh master info",
						zap.String("worker-id", w.workerID),
						zap.String("master-id", w.masterInfo.MasterID()),
						zap.Error(err))
				}
			}
			log.Warn("failed to send status to master. Retrying...",
				zap.String("worker-id", w.workerID),
				zap.String("master-id", w.masterInfo.MasterID()),
				zap.Any("status", newStatus),
				zap.Error(err))
			continue
		}
		return nil
	}
}

func (w *Writer) persistStatus(ctx context.Context, newStatus *frameModel.WorkerStatus) error {
	return retry.Do(ctx, func() error {
		return w.metaclient.UpdateWorker(ctx, newStatus)
	}, retry.WithBackoffMaxDelay(1000 /* 1 second */), retry.WithIsRetryableErr(func(err error) bool {
		// TODO: refine the IsRetryable method
		//if err, ok := err.(metaclient.Error); ok {
		//return err.IsRetryable()
		//}
		return true
	}))
}

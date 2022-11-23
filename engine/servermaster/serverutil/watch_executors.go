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

package serverutil

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"go.uber.org/zap"
)

// executorWatcher represents an object that can provide a snapshot
// of the executor list followed by a stream of updates to the list.
type executorWatcher interface {
	WatchExecutors(ctx context.Context) (
		snap map[model.ExecutorID]string,
		stream *notifier.Receiver[model.ExecutorStatusChange],
		err error,
	)
}

// executorInfoUser represents an object that uses the information provides
// by executorWatcher.
type executorInfoUser interface {
	UpdateExecutorList(executors map[model.ExecutorID]string) error
	AddExecutor(executorID model.ExecutorID, addr string) error
	RemoveExecutor(executorID model.ExecutorID) error
}

// WatchExecutors updates the state of `user` with information provides
// by `watcher`. It blocks until canceled or an error is encountered.
func WatchExecutors(ctx context.Context, watcher executorWatcher, user executorInfoUser) (retErr error) {
	defer func() {
		log.Info("WatchExecutors exited", logutil.ShortError(retErr))
	}()

	watchStart := time.Now()
	snap, updates, err := watcher.WatchExecutors(ctx)

	if duration := time.Since(watchStart); duration >= 100*time.Millisecond {
		log.Warn("WatchExecutors took too long",
			zap.Duration("duration", duration),
			zap.Any("snap", snap))
	}
	if err != nil {
		return errors.Annotate(err, "watch executors")
	}
	defer updates.Close()

	log.Info("update executor list", zap.Any("list", snap))
	if err := user.UpdateExecutorList(snap); err != nil {
		return errors.Annotate(err, "watch executors")
	}

	for {
		var (
			change model.ExecutorStatusChange
			ok     bool
		)
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case change, ok = <-updates.C:
		}

		if !ok {
			return errors.ErrExecutorWatcherClosed.GenWithStackByArgs()
		}
		if change.Tp == model.EventExecutorOnline {
			err := user.AddExecutor(change.ID, change.Addr)
			if err != nil {
				if errors.Is(err, errors.ErrExecutorAlreadyExists) {
					log.Warn("unexpected EventExecutorOnline",
						zap.Error(err))
					continue
				}
				return err
			}
			log.Info("add executor",
				zap.String("id", string(change.ID)),
				zap.String("addr", change.Addr))
		} else if change.Tp == model.EventExecutorOffline {
			err := user.RemoveExecutor(change.ID)
			if err != nil {
				if errors.Is(err, errors.ErrExecutorNotFound) || errors.Is(err, errors.ErrTombstoneExecutor) {
					log.Warn("unexpected EventExecutorOffline",
						zap.Error(err))
					continue
				}
				return errors.Annotate(err, "watch executors")
			}
			log.Info("remove executor",
				zap.String("id", string(change.ID)))
		} else {
			log.Panic("unknown ExecutorStatusChange type",
				zap.Any("change", change))
		}
	}
}

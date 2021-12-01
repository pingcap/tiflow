// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

const (
	defaultErrChSize = 1024
)

// AsyncSink is an async sink design for owner
// The EmitCheckpointTs and EmitDDLEvent is asynchronous function for now
// Other functions are still synchronization
type AsyncSink interface {
	// EmitCheckpointTs emits the checkpoint Ts to downstream data source
	// this function will return after recording the checkpointTs specified in memory immediately
	// and the recorded checkpointTs will be sent and updated to downstream data source every second
	EmitCheckpointTs(ctx cdcContext.Context, ts uint64)
	// EmitDDLEvent emits DDL event asynchronously and return true if the DDL is executed
	// the DDL event will be sent to another goroutine and execute to downstream
	// the caller of this function can call again and again until a true returned
	EmitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error)
	SinkSyncpoint(ctx cdcContext.Context, checkpointTs uint64) error
	Close(ctx context.Context) error
}

type asyncSinkImpl struct {
	sink           sink.Sink
	syncpointStore sink.SyncpointStore

	checkpointTs model.Ts

	lastSyncPoint model.Ts

	ddlCh         chan *model.DDLEvent
	ddlFinishedTs model.Ts
	ddlSentTs     model.Ts

	cancel context.CancelFunc
	wg     sync.WaitGroup
	errCh  chan error

	initialized chan struct{}
	sinkMu      sync.Mutex
}

type asyncSinkInitFunc func(a *asyncSinkImpl) error

func newAsyncSink(ctx cdcContext.Context, initializer asyncSinkInitFunc) (AsyncSink, error) {
	ctx, cancel := cdcContext.WithCancel(ctx)

	errCh := make(chan error, defaultErrChSize)
	asyncSink := &asyncSinkImpl{
		sink:        nil,
		ddlCh:       make(chan *model.DDLEvent, 1),
		errCh:       errCh,
		cancel:      cancel,
		initialized: make(chan struct{}),
	}

	asyncSink.wg.Add(1)
	go func() {
		defer asyncSink.wg.Done()
		errCh <- initializer(asyncSink)
	}()

	var (
		err  error
		id   = ctx.ChangefeedVars().ID
		info = ctx.ChangefeedVars().Info
	)
	if info.SyncPointEnabled {
		asyncSink.syncpointStore, err = sink.NewSyncpointStore(ctx, id, info.SinkURI)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err := asyncSink.syncpointStore.CreateSynctable(ctx); err != nil {
			return nil, errors.Trace(err)
		}
	}
	asyncSink.wg.Add(1)
	go asyncSink.run(ctx)
	return asyncSink, nil
}

func (s *asyncSinkImpl) attachSink(backendSink sink.Sink) {
	s.sinkMu.Lock()
	defer s.sinkMu.Unlock()
	s.sink = backendSink
	close(s.initialized)
}

func (s *asyncSinkImpl) waitSinkInitialized(ctx cdcContext.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-s.errCh:
			return err
		case <-s.initialized:
			return nil
		}
	}
}

func (s *asyncSinkImpl) run(ctx cdcContext.Context) {
	defer s.wg.Done()
	if err := s.waitSinkInitialized(ctx); err != nil {
		ctx.Throw(err)
		return
	}

	// TODO make the tick duration configurable
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastCheckpointTs model.Ts
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-s.errCh:
			ctx.Throw(err)
			return
		case <-ticker.C:
			checkpointTs := atomic.LoadUint64(&s.checkpointTs)
			if checkpointTs == 0 || checkpointTs <= lastCheckpointTs {
				continue
			}
			lastCheckpointTs = checkpointTs
			if err := s.sink.EmitCheckpointTs(ctx, checkpointTs); err != nil {
				ctx.Throw(errors.Trace(err))
				return
			}
		case ddl := <-s.ddlCh:
			err := s.sink.EmitDDLEvent(ctx, ddl)
			failpoint.Inject("InjectChangefeedDDLError", func() {
				err = cerror.ErrExecDDLFailed.GenWithStackByArgs()
			})
			if err == nil || cerror.ErrDDLEventIgnored.Equal(errors.Cause(err)) {
				log.Info("Execute DDL succeeded", zap.String("changefeed", ctx.ChangefeedVars().ID), zap.Bool("ignored", err != nil), zap.Reflect("ddl", ddl))
				atomic.StoreUint64(&s.ddlFinishedTs, ddl.CommitTs)
			} else {
				// If DDL executing failed, and the error can not be ignored, throw an error and pause the changefeed
				log.Error("Execute DDL failed",
					zap.String("ChangeFeedID", ctx.ChangefeedVars().ID),
					zap.Error(err),
					zap.Reflect("ddl", ddl))
				ctx.Throw(errors.Trace(err))
				return
			}
		}
	}
}

func (s *asyncSinkImpl) EmitCheckpointTs(ctx cdcContext.Context, ts uint64) {
	atomic.StoreUint64(&s.checkpointTs, ts)
}

func (s *asyncSinkImpl) EmitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error) {
	ddlFinishedTs := atomic.LoadUint64(&s.ddlFinishedTs)
	if ddl.CommitTs <= ddlFinishedTs {
		// the DDL event is executed successfully, and done is true
		return true, nil
	}
	if ddl.CommitTs <= s.ddlSentTs {
		// the DDL event is executing and not finished yes, return false
		return false, nil
	}
	select {
	case <-ctx.Done():
		return false, errors.Trace(ctx.Err())
	case s.ddlCh <- ddl:
	}
	s.ddlSentTs = ddl.CommitTs
	return false, nil
}

func (s *asyncSinkImpl) SinkSyncpoint(ctx cdcContext.Context, checkpointTs uint64) error {
	if checkpointTs == s.lastSyncPoint {
		return nil
	}
	s.lastSyncPoint = checkpointTs
	// TODO implement async sink syncpoint
	return s.syncpointStore.SinkSyncpoint(ctx, ctx.ChangefeedVars().ID, checkpointTs)
}

func (s *asyncSinkImpl) Close(ctx context.Context) (err error) {
	s.cancel()
	err = s.sink.Close(ctx)
	if s.syncpointStore != nil {
		err = s.syncpointStore.Close()
	}
	s.wg.Wait()
	return
}

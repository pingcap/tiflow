//  Copyright 2023 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package memory

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/model/codec"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	eventPool = sync.Pool{
		New: func() interface{} {
			return &polymorphicRedoEvent{}
		},
	}
	dataPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

type polymorphicRedoEvent struct {
	// event is the redo event (model.RowChangedEvent or model.DDLEvent) to be encoded.
	event writer.RedoEvent
	// data is the encoded redo event.
	data     *bytes.Buffer
	commitTs model.Ts

	flushCallback func()
}

func newFlushPolymorphicRedoLog(fn func()) *polymorphicRedoEvent {
	return &polymorphicRedoEvent{
		flushCallback: fn,
	}
}

func (e *polymorphicRedoEvent) reset() {
	e.event = nil
	dataPool.Put(e.data)
	e.data = nil
	e.commitTs = 0
}

// encoding format: lenField(8 bytes) + rawData + padding bytes(force 8 bytes alignment)
func (e *polymorphicRedoEvent) encode() (err error) {
	redoLog := e.event.ToRedoLog()
	e.commitTs = redoLog.GetCommitTs()

	rawData, err := codec.MarshalRedoLog(redoLog, nil)
	if err != nil {
		return err
	}
	uint64buf := make([]byte, 8)
	lenField, padBytes := writer.EncodeFrameSize(len(rawData))
	binary.LittleEndian.PutUint64(uint64buf, lenField)

	e.data = dataPool.Get().(*bytes.Buffer)
	e.data.Reset()
	_, err = e.data.Write(uint64buf)
	if err != nil {
		return err
	}
	_, err = e.data.Write(rawData)
	if err != nil {
		return err
	}
	if padBytes != 0 {
		_, err = e.data.Write(make([]byte, padBytes))
	}

	e.event = nil
	return err
}

type encodingWorkerGroup struct {
	changefeed model.ChangeFeedID
	outputCh   chan *polymorphicRedoEvent
	inputChs   []chan *polymorphicRedoEvent
	workerNum  int
	nextWorker atomic.Uint64

	closed chan struct{}
}

func newEncodingWorkerGroup(cfg *writer.LogWriterConfig) *encodingWorkerGroup {
	workerNum := cfg.EncodingWorkerNum
	if workerNum <= 0 {
		workerNum = redo.DefaultEncodingWorkerNum
	}
	inputChs := make([]chan *polymorphicRedoEvent, workerNum)
	for i := 0; i < workerNum; i++ {
		inputChs[i] = make(chan *polymorphicRedoEvent, redo.DefaultEncodingInputChanSize)
	}
	return &encodingWorkerGroup{
		changefeed: cfg.ChangeFeedID,
		inputChs:   inputChs,
		outputCh:   make(chan *polymorphicRedoEvent, redo.DefaultEncodingOutputChanSize),
		workerNum:  workerNum,
		closed:     make(chan struct{}),
	}
}

func (e *encodingWorkerGroup) Run(ctx context.Context) (err error) {
	defer func() {
		close(e.closed)
		if err != nil && errors.Cause(err) != context.Canceled {
			log.Warn("redo fileWorkerGroup closed with error",
				zap.String("namespace", e.changefeed.Namespace),
				zap.String("changefeed", e.changefeed.ID),
				zap.Error(err))
		}
	}()
	eg, egCtx := errgroup.WithContext(ctx)
	for i := 0; i < e.workerNum; i++ {
		idx := i
		eg.Go(func() error {
			return e.runWorker(egCtx, idx)
		})
	}
	log.Info("redo log encoding workers started",
		zap.String("namespace", e.changefeed.Namespace),
		zap.String("changefeed", e.changefeed.ID),
		zap.Int("workerNum", e.workerNum))
	return eg.Wait()
}

func (e *encodingWorkerGroup) AddEvent(ctx context.Context, event writer.RedoEvent) error {
	redoEvent := eventPool.Get().(*polymorphicRedoEvent)
	redoEvent.event = event
	idx := e.nextWorker.Add(1) % uint64(e.workerNum)
	return e.input(ctx, idx, redoEvent)
}

func (e *encodingWorkerGroup) runWorker(egCtx context.Context, idx int) error {
	for {
		select {
		case <-egCtx.Done():
			return errors.Trace(egCtx.Err())
		case event := <-e.inputChs[idx]:
			if event.event != nil {
				if err := event.encode(); err != nil {
					return errors.Trace(err)
				}
				if err := e.output(egCtx, event); err != nil {
					return errors.Trace(err)
				}
			}
			if event.flushCallback != nil {
				event.flushCallback()
			}
		}
	}
}

func (e *encodingWorkerGroup) input(
	ctx context.Context, idx uint64, event *polymorphicRedoEvent,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closed:
		return errors.ErrRedoWriterStopped.GenWithStack("encoding worker is closed")
	case e.inputChs[idx] <- event:
		return nil
	}
}

func (e *encodingWorkerGroup) output(
	ctx context.Context, event *polymorphicRedoEvent,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closed:
		return errors.ErrRedoWriterStopped.GenWithStack("encoding worker is closed")
	case e.outputCh <- event:
		return nil
	}
}

func (e *encodingWorkerGroup) FlushAll(ctx context.Context) error {
	if err := e.broadcastAndWaitEncoding(ctx); err != nil {
		return err
	}

	// notify file worker to flush
	flushCh := make(chan struct{})
	flushEvent := newFlushPolymorphicRedoLog(func() {
		close(flushCh)
	})
	if err := e.output(ctx, flushEvent); err != nil {
		return err
	}

	// wait all file flushed to external storage
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closed:
		return errors.ErrRedoWriterStopped.GenWithStack("encoding worker is closed")
	case <-flushCh:
	}
	return nil
}

func (e *encodingWorkerGroup) broadcastAndWaitEncoding(ctx context.Context) error {
	flushChs := make([]chan struct{}, e.workerNum)
	for i := 0; i < e.workerNum; i++ {
		ch := make(chan struct{})
		flushEvent := newFlushPolymorphicRedoLog(func() {
			close(ch)
		})
		if err := e.input(ctx, uint64(i), flushEvent); err != nil {
			return err
		}
		flushChs[i] = ch
	}

	for _, ch := range flushChs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.closed:
			return errors.ErrRedoWriterStopped.GenWithStack("encoding worker is closed")
		case <-ch:
		}
	}
	return nil
}

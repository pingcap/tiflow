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

package blob

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultBufferChanSize               = 20480
	defaultFlushRowChangedEventInterval = 1 * time.Second // TODO make it as a config
)

type tableBuffer struct {
	tableID    int64
	dataCh     chan *model.RowChangedEvent
	sendSize   *atomic.Int64
	sendEvents *atomic.Int64
}

func (tb *tableBuffer) dataChan() chan *model.RowChangedEvent {
	return tb.dataCh
}

func (tb *tableBuffer) TableID() int64 {
	return tb.tableID
}

func (tb *tableBuffer) Events() *atomic.Int64 {
	return tb.sendEvents
}

func (tb *tableBuffer) Size() *atomic.Int64 {
	return tb.sendSize
}

func (tb *tableBuffer) isEmpty() bool {
	return tb.sendEvents.Load() == 0
}

func (tb *tableBuffer) shouldFlush() bool {
	// if data chennal is full, flush it
	return tb.sendEvents.Load() == defaultBufferChanSize
}

func (tb *tableBuffer) flush(ctx context.Context, sink *blobSink) error {
	sendEvents := tb.sendEvents.Load()
	if sendEvents == 0 {
		log.Info("nothing to flush", zap.Int64("tableID", tb.tableID))
		return nil
	}

	var newFileName string
	flushedSize := int64(0)
	encoder := sink.encoderBuilder.Build()
	for event := int64(0); event < sendEvents; event++ {
		row := <-tb.dataCh
		flushedSize += row.ApproximateDataSize
		if event == sendEvents-1 {
			// if last event, we record ts as new rotate file name
			newFileName = makeTableFileObject(row.Table.TableID, row.CommitTs)
		}
		err := encoder.AppendRowChangedEvent(ctx, "", row)
		if err != nil {
			return err
		}
	}
	rowDatas := makeBlobStorageContent(encoder.Build())

	log.Debug("[FlushRowChangedEvents[Debug]] flush table buffer",
		zap.Int64("table", tb.tableID),
		zap.Int64("event size", sendEvents),
		zap.Int("row data size", len(rowDatas)),
	)
	err := sink.storage.WriteFile(ctx, newFileName, rowDatas)
	if err != nil {
		return cerror.WrapError(cerror.ErrBlobSinkStorageAPI, err)
	}

	tb.sendEvents.Sub(sendEvents)
	tb.sendSize.Sub(flushedSize)
	return nil
}

func newTableBuffer(tableID int64) *tableBuffer {
	return &tableBuffer{
		tableID:    tableID,
		dataCh:     make(chan *model.RowChangedEvent, defaultBufferChanSize),
		sendSize:   atomic.NewInt64(0),
		sendEvents: atomic.NewInt64(0),
	}
}

type blobSink struct {
	notifyChan     chan []*tableBuffer
	notifyWaitChan chan struct{}

	encoderBuilder codec.EncoderBuilder
	tableBufferMap sync.Map

	storage storage.ExternalStorage
	logMeta *logMeta
}

func (b *blobSink) AddTable(tableID model.TableID) error {
	// AddTable does nothing because no table stats need to be cleared.
	return nil
}

func (b *blobSink) RemoveTable(cxt context.Context, tableID model.TableID) error {
	// RemoveTable does nothing because FlushRowChangedEvents in s3 sink had flushed
	// all buffered events by force.
	return nil
}

func (b *blobSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		// dispatch row event by tableID
		tableID := row.Table.GetTableID()
		rawTable, _ := b.tableBufferMap.LoadOrStore(tableID, newTableBuffer(tableID))
		table := rawTable.(*tableBuffer)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case table.dataChan() <- row:
			table.Size().Add(row.ApproximateDataSize)
			table.Events().Inc()
		}
	}
	return nil
}

func (b *blobSink) flushLogMeta(ctx context.Context) error {
	data, err := b.logMeta.Marshal()
	if err != nil {
		return cerror.WrapError(cerror.ErrMarshalFailed, err)
	}
	return cerror.WrapError(cerror.ErrBlobSinkStorageAPI, b.storage.WriteFile(ctx, logMetaFile, data))
}

func (b *blobSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolvedTs uint64) (uint64, error) {
	// Every time we get a flush request, we flush all the tables to the blob storage.
	needFlushedTables := make([]*tableBuffer, 0)
	b.tableBufferMap.Range(func(k, v any) bool {
		t := v.(*tableBuffer)
		if !t.isEmpty() {
			needFlushedTables = append(needFlushedTables, t)
		}
		return true
	})
	if len(needFlushedTables) > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case b.notifyChan <- needFlushedTables:
			// wait flush worker finished
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-b.notifyWaitChan:
			}
		}
	}
	return resolvedTs, nil
}

// EmitCheckpointTs update the global resolved ts and table mapping in log meta
func (b *blobSink) EmitCheckpointTs(ctx context.Context, ts uint64, tables []model.TableName) error {
	b.logMeta = makeLogMetaContent(tables)
	b.logMeta.GlobalResolvedTS = ts
	return b.flushLogMeta(ctx)
}

// EmitDDLEvent write ddl event to S3 directory, all events split by '\n'
func (b *blobSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	// reset encoder buf for next round append
	ddlEncoder := b.encoderBuilder.Build()

	_, er := ddlEncoder.EncodeDDLEvent(ddl)
	if er != nil {
		return er
	}

	data := makeBlobStorageContent(ddlEncoder.Build())

	name := makeDDLFileObject(ddl.CommitTs)
	log.Debug("[EmitDDLEvent] create ddl log",
		zap.String("name", name), zap.Any("ddl", ddl))
	return b.storage.WriteFile(ctx, name, data)
}

func (b *blobSink) Close(ctx context.Context) error {
	return nil
}

func (b *blobSink) Barrier(ctx context.Context, tableID model.TableID) error {
	// Barrier does nothing because FlushRowChangedEvents in s3 sink has flushed
	// all buffered events forcefully.
	return nil
}

func (b *blobSink) startFlush(ctx context.Context) error {
	ticker := time.NewTicker(defaultFlushRowChangedEventInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("[startFlush] s3 sink stopped")
			return ctx.Err()
		case needFlushedTables := <-b.notifyChan:
			// try specific tables
			eg, ectx := errgroup.WithContext(ctx)
			for _, t := range needFlushedTables {
				tReplica := t
				eg.Go(func() error {
					log.Info("start Flush asynchronously to storage by caller",
						zap.Int64("table id", tReplica.TableID()),
						zap.Int64("size", tReplica.Size().Load()),
						zap.Int64("event count", tReplica.Events().Load()),
					)
					return tReplica.flush(ectx, b)
				})
			}
			if err := eg.Wait(); err != nil {
				return err
			}
			// tell flush goroutine this time flush finished
			b.notifyWaitChan <- struct{}{}

		case <-ticker.C:
			// try all tableBuffers
			eg, ectx := errgroup.WithContext(ctx)
			b.tableBufferMap.Range(func(k, v any) bool {
				t := v.(*tableBuffer)
				if t.shouldFlush() {
					eg.Go(func() error {
						log.Info("start Flush asynchronously to storage",
							zap.Int64("table id", t.TableID()),
							zap.Int64("size", t.Size().Load()),
							zap.Int64("event count", t.Events().Load()),
						)
						return t.flush(ectx, b)
					})
				}
				return true
			})
			if err := eg.Wait(); err != nil {
				return err
			}
		}
	}
}

// NewS3Sink creates new sink support log data to s3 directly
func NewS3Sink(ctx context.Context, sinkURI *url.URL, errCh chan error) (*blobSink, error) {
	if len(sinkURI.Host) == 0 {
		return nil, errors.Errorf("please specify the bucket for s3 in %s", sinkURI)
	}
	prefix := strings.Trim(sinkURI.Path, "/")
	s3 := &backup.S3{Bucket: sinkURI.Host, Prefix: prefix}
	options := &storage.BackendOptions{}
	storage.ExtractQueryParameters(sinkURI, &options.S3)
	if err := options.S3.Apply(s3); err != nil {
		return nil, cerror.WrapError(cerror.ErrS3SinkInitialize, err)
	}
	// we should set this to true, since br set it by default in parseBackend
	s3.ForcePathStyle = true
	backend := &backup.StorageBackend{
		Backend: &backup.StorageBackend_S3{S3: s3},
	}
	s3storage, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{
		SendCredentials: false,
		HTTPClient:      nil,
	})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrS3SinkInitialize, err)
	}

	// Only supports open protocol for now
	encoderBuilder, err := codec.NewEventBatchEncoderBuilder(ctx, codec.NewConfig(config.ProtocolOpen))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrS3SinkInitialize, err)
	}

	b := &blobSink{
		notifyChan:     make(chan []*tableBuffer),
		notifyWaitChan: make(chan struct{}),
		encoderBuilder: encoderBuilder,
		storage:        s3storage,
		logMeta:        newLogMeta(),
	}

	// important! we should flush asynchronously in another goroutine
	go func() {
		if err := b.startFlush(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()

	return b, nil
}

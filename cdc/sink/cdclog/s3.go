// Copyright 2020 PingCAP, Inc.
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

package cdclog

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	parsemodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/quotes"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

const (
	maxPartFlushSize    = 5 << 20   // The minimal multipart upload size is 5Mb.
	maxCompletePartSize = 100 << 20 // rotate row changed event file if one complete file larger than 100Mb
	maxDDLFlushSize     = 10 << 20  // rotate ddl event file if one complete file larger than 10Mb

	defaultBufferChanSize               = 20480
	defaultFlushRowChangedEventDuration = 5 * time.Second // TODO make it as a config
)

type tableBuffer struct {
	// for log
	tableID    int64
	dataCh     chan *model.RowChangedEvent
	sendSize   *atomic.Int64
	sendEvents *atomic.Int64

	encoder codec.EventBatchEncoder

	uploadParts struct {
		uploader  storage.Uploader
		uploadNum int
		byteSize  int64
	}
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
	return tb.sendEvents.Load() == 0 && tb.uploadParts.uploadNum == 0
}

func (tb *tableBuffer) shouldFlush() bool {
	return tb.sendSize.Load() > maxPartFlushSize
}

func (tb *tableBuffer) flush(ctx context.Context, sink *logSink) error {
	hashPart := tb.uploadParts
	sendEvents := tb.sendEvents.Load()
	if sendEvents == 0 && hashPart.uploadNum == 0 {
		log.Info("nothing to flush", zap.Int64("tableID", tb.tableID))
		return nil
	}

	firstCreated := false
	if tb.encoder == nil {
		// create encoder for each file
		tb.encoder = sink.encoder()
		firstCreated = true
	}

	var newFileName string
	flushedSize := int64(0)
	for event := int64(0); event < sendEvents; event++ {
		row := <-tb.dataCh
		flushedSize += row.ApproximateSize
		if event == sendEvents-1 {
			// if last event, we record ts as new rotate file name
			newFileName = makeTableFileObject(row.Table.TableID, row.CommitTs)
		}
		_, err := tb.encoder.AppendRowChangedEvent(row)
		if err != nil {
			return err
		}
	}
	rowDatas := tb.encoder.MixedBuild(firstCreated)
	// reset encoder buf for next round append
	defer func() {
		if tb.encoder != nil {
			tb.encoder.Reset()
		}
	}()

	log.Debug("[FlushRowChangedEvents[Debug]] flush table buffer",
		zap.Int64("table", tb.tableID),
		zap.Int64("event size", sendEvents),
		zap.Int("row data size", len(rowDatas)),
		zap.Int("upload num", hashPart.uploadNum),
		zap.Int64("upload byte size", hashPart.byteSize),
		// zap.ByteString("rowDatas", rowDatas),
	)

	if len(rowDatas) > maxPartFlushSize || hashPart.uploadNum > 0 {
		// S3 multi-upload need every chunk(except the last one) is greater than 5Mb
		// so, if this batch data size is greater than 5Mb or it has uploadPart already
		// we will use multi-upload this batch data
		if len(rowDatas) > 0 {
			if hashPart.uploader == nil {
				uploader, err := sink.storage().CreateUploader(ctx, newFileName)
				if err != nil {
					return cerror.WrapError(cerror.ErrS3SinkStorageAPI, err)
				}
				hashPart.uploader = uploader
			}

			err := hashPart.uploader.UploadPart(ctx, rowDatas)
			if err != nil {
				return cerror.WrapError(cerror.ErrS3SinkStorageAPI, err)
			}

			hashPart.byteSize += int64(len(rowDatas))
			hashPart.uploadNum++
		}

		if hashPart.byteSize > maxCompletePartSize || len(rowDatas) <= maxPartFlushSize {
			// we need do complete when total upload size is greater than 100Mb
			// or this part data is less than 5Mb to avoid meet EntityTooSmall error
			log.Info("[FlushRowChangedEvents] complete file", zap.Int64("tableID", tb.tableID))
			err := hashPart.uploader.CompleteUpload(ctx)
			if err != nil {
				return cerror.WrapError(cerror.ErrS3SinkStorageAPI, err)
			}
			hashPart.byteSize = 0
			hashPart.uploadNum = 0
			hashPart.uploader = nil
			tb.encoder = nil
		}
	} else {
		// generate normal file because S3 multi-upload need every part at least 5Mb.
		log.Info("[FlushRowChangedEvents] normal upload file", zap.Int64("tableID", tb.tableID))
		err := sink.storage().Write(ctx, newFileName, rowDatas)
		if err != nil {
			return cerror.WrapError(cerror.ErrS3SinkStorageAPI, err)
		}
		tb.encoder = nil
	}

	tb.sendEvents.Sub(sendEvents)
	tb.sendSize.Sub(flushedSize)
	tb.uploadParts = hashPart
	return nil
}

func newTableBuffer(tableID int64) logUnit {
	return &tableBuffer{
		tableID:    tableID,
		dataCh:     make(chan *model.RowChangedEvent, defaultBufferChanSize),
		sendSize:   atomic.NewInt64(0),
		sendEvents: atomic.NewInt64(0),
		uploadParts: struct {
			uploader  storage.Uploader
			uploadNum int
			byteSize  int64
		}{
			uploader:  nil,
			uploadNum: 0,
			byteSize:  0,
		},
	}
}

type s3Sink struct {
	*logSink

	prefix string

	storage storage.ExternalStorage

	logMeta *logMeta

	// hold encoder for ddl event log
	ddlEncoder codec.EventBatchEncoder
}

func (s *s3Sink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	return s.emitRowChangedEvents(ctx, newTableBuffer, rows...)
}

func (s *s3Sink) flushLogMeta(ctx context.Context) error {
	data, err := s.logMeta.Marshal()
	if err != nil {
		return cerror.WrapError(cerror.ErrMarshalFailed, err)
	}
	return cerror.WrapError(cerror.ErrS3SinkWriteStorage, s.storage.Write(ctx, logMetaFile, data))
}

func (s *s3Sink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	// we should flush all events before resolvedTs, there are two kind of flush policy
	// 1. flush row events to a s3 chunk: if the event size is not enough,
	//    TODO: when cdc crashed, we should repair these chunks to a complete file
	// 2. flush row events to a complete s3 file: if the event size is enough
	return s.flushRowChangedEvents(ctx, resolvedTs)
}

// EmitCheckpointTs update the global resolved ts in log meta
// sleep 5 seconds to avoid update too frequently
func (s *s3Sink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	s.logMeta.GlobalResolvedTS = ts
	return s.flushLogMeta(ctx)
}

// EmitDDLEvent write ddl event to S3 directory, all events split by '\n'
// Because S3 doesn't support append-like write.
// we choose a hack way to read origin file then write in place.
func (s *s3Sink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	switch ddl.Type {
	case parsemodel.ActionCreateTable:
		s.logMeta.Names[ddl.TableInfo.TableID] = quotes.QuoteSchema(ddl.TableInfo.Schema, ddl.TableInfo.Table)
		err := s.flushLogMeta(ctx)
		if err != nil {
			return err
		}
	case parsemodel.ActionRenameTable:
		delete(s.logMeta.Names, ddl.PreTableInfo.TableID)
		s.logMeta.Names[ddl.TableInfo.TableID] = quotes.QuoteSchema(ddl.TableInfo.Schema, ddl.TableInfo.Table)
		err := s.flushLogMeta(ctx)
		if err != nil {
			return err
		}
	}
	firstCreated := false
	if s.ddlEncoder == nil {
		s.ddlEncoder = s.encoder()
		firstCreated = true
	}
	_, err := s.ddlEncoder.EncodeDDLEvent(ddl)
	if err != nil {
		return err
	}
	data := s.ddlEncoder.MixedBuild(firstCreated)
	// reset encoder buf for next round append
	defer s.ddlEncoder.Reset()

	var (
		name     string
		size     int64
		fileData []byte
	)
	opt := &storage.WalkOption{
		SubDir:    ddlEventsDir,
		ListCount: 1,
	}
	err = s.storage.WalkDir(ctx, opt, func(key string, fileSize int64) error {
		log.Debug("[EmitDDLEvent] list content from s3",
			zap.String("key", key),
			zap.Int64("size", size),
			zap.Any("ddl", ddl))
		name = strings.ReplaceAll(key, s.prefix, "")
		size = fileSize
		return nil
	})
	if err != nil {
		return cerror.WrapError(cerror.ErrS3SinkStorageAPI, err)
	}
	if size == 0 || size > maxDDLFlushSize {
		// no ddl file exists or
		// exists file is oversized. we should generate a new file
		fileData = data
		name = makeDDLFileObject(ddl.CommitTs)
		log.Debug("[EmitDDLEvent] create first or rotate ddl log",
			zap.String("name", name), zap.Any("ddl", ddl))
		if size > maxDDLFlushSize {
			// reset ddl encoder for new file
			s.ddlEncoder = nil
		}
	} else {
		// hack way: append data to old file
		log.Debug("[EmitDDLEvent] append ddl to origin log",
			zap.String("name", name), zap.Any("ddl", ddl))
		fileData, err = s.storage.Read(ctx, name)
		if err != nil {
			return cerror.WrapError(cerror.ErrS3SinkStorageAPI, err)
		}
		fileData = append(fileData, data...)
	}
	return s.storage.Write(ctx, name, fileData)
}

func (s *s3Sink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	if tableInfo != nil {
		for _, table := range tableInfo {
			if table != nil {
				err := s.storage.Write(ctx, makeTableDirectoryName(table.TableID), nil)
				if err != nil {
					return errors.Annotate(
						cerror.WrapError(cerror.ErrS3SinkStorageAPI, err),
						"create table directory on s3 failed")
				}
			}
		}
		// update log meta to record the relationship about tableName and tableID
		s.logMeta = makeLogMetaContent(tableInfo)

		data, err := s.logMeta.Marshal()
		if err != nil {
			return cerror.WrapError(cerror.ErrMarshalFailed, err)
		}
		return s.storage.Write(ctx, logMetaFile, data)
	}
	return nil
}

func (s *s3Sink) Close() error {
	return nil
}

// NewS3Sink creates new sink support log data to s3 directly
func NewS3Sink(ctx context.Context, sinkURI *url.URL, errCh chan error) (*s3Sink, error) {
	if len(sinkURI.Host) == 0 {
		return nil, errors.Errorf("please specify the bucket for s3 in %s", sinkURI)
	}
	prefix := strings.Trim(sinkURI.Path, "/")
	s3 := &backup.S3{Bucket: sinkURI.Host, Prefix: prefix}
	options := &storage.BackendOptions{}
	storage.ExtractQueryParameters(sinkURI, &options.S3)
	if err := options.S3.Apply(s3); err != nil {
		return nil, cerror.WrapError(cerror.ErrS3SinkInitialzie, err)
	}
	// we should set this to true, since br set it by default in parseBackend
	s3.ForcePathStyle = true
	backend := &backup.StorageBackend{
		Backend: &backup.StorageBackend_S3{S3: s3},
	}
	s3storage, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{
		SendCredentials: false,
		SkipCheckPath:   false,
		HTTPClient:      nil,
	})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrS3SinkInitialzie, err)
	}

	s := &s3Sink{
		prefix:  prefix,
		storage: s3storage,
		logMeta: newLogMeta(),
		logSink: newLogSink("", s3storage),
	}

	// important! we should flush asynchronously in another goroutine
	go func() {
		if err := s.startFlush(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()

	return s, nil
}

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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	parsemodel "github.com/pingcap/parser/model"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
)

const (
	maxNotifySize       = 15 << 20  // trigger flush if one table has reached 16Mb data size in memory
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

	uploadParts struct {
		originPartUploadResponse *s3.CreateMultipartUploadOutput
		byteSize                 int64
		completeParts            []*s3.CompletedPart
	}
}

func (tb *tableBuffer) flush(ctx context.Context, s *s3Sink) error {
	hashPart := tb.uploadParts
	sendEvents := tb.sendEvents.Load()
	if sendEvents == 0 && len(hashPart.completeParts) == 0 {
		log.Info("nothing to flush", zap.Int64("tableID", tb.tableID))
		return nil
	}

	var newFileName string
	flushedSize := int64(0)
	encoder := s.encoder()
	for event := int64(0); event < sendEvents; event++ {
		row := <-tb.dataCh
		flushedSize += row.ApproximateSize
		if event == sendEvents-1 {
			// if last event, we record ts as new rotate file name
			newFileName = makeTableFileObject(row.Table.TableID, row.CommitTs)
		}
		_, err := encoder.AppendRowChangedEvent(row)
		if err != nil {
			return err
		}
	}
	rowDatas := encoder.MixedBuild()
	log.Debug("[FlushRowChangedEvents[Debug]] flush table buffer",
		zap.Int64("table", tb.tableID),
		zap.Int64("event size", sendEvents),
		zap.Int("row data size", len(rowDatas)),
		zap.Int("upload num", len(hashPart.completeParts)),
		zap.Int64("upload byte size", hashPart.byteSize),
		// zap.ByteString("rowDatas", rowDatas),
	)

	if len(rowDatas) > 0 {
		if len(rowDatas) > maxPartFlushSize || len(hashPart.completeParts) > 0 {
			// S3 multi-upload need every chunk(except the last one) is greater than 5Mb
			// so, if this batch data size is greater than 5Mb or it has uploadPart already
			// we will use multi-upload this batch data
			if hashPart.originPartUploadResponse == nil {
				resp, err := s.storage.CreateMultipartUpload(ctx, newFileName)
				if err != nil {
					return err
				}
				hashPart.originPartUploadResponse = resp
			}

			completePart, err := s.storage.UploadPart(ctx, hashPart.originPartUploadResponse, rowDatas, len(tb.uploadParts.completeParts))
			if err != nil {
				return err
			}

			hashPart.byteSize += int64(len(rowDatas))
			hashPart.completeParts = append(hashPart.completeParts, completePart)

			if hashPart.byteSize > maxCompletePartSize || len(rowDatas) <= maxPartFlushSize {
				// we need do complete when total upload size is greater than 100Mb
				// or this part data is less than 5Mb to avoid meet EntityTooSmall error
				log.Info("[FlushRowChangedEvents] complete file",
					zap.Int64("tableID", tb.tableID),
					zap.Stringer("resp", hashPart.originPartUploadResponse))
				_, err := s.storage.CompleteMultipartUpload(ctx, hashPart.originPartUploadResponse, hashPart.completeParts)
				if err != nil {
					return err
				}
				hashPart.byteSize = 0
				hashPart.completeParts = hashPart.completeParts[:0]
				hashPart.originPartUploadResponse = nil
			}
		} else {
			// generate normal file because S3 multi-upload need every part at least 5Mb.
			log.Info("[FlushRowChangedEvents] normal upload file", zap.Int64("tableID", tb.tableID))
			err := s.storage.Write(ctx, newFileName, rowDatas)
			if err != nil {
				return err
			}
		}
	}

	tb.sendEvents.Sub(sendEvents)
	tb.sendSize.Sub(flushedSize)
	tb.uploadParts = hashPart
	return nil
}

func newTableBuffer(tableID int64) *tableBuffer {
	return &tableBuffer{
		tableID:    tableID,
		dataCh:     make(chan *model.RowChangedEvent, defaultBufferChanSize),
		sendSize:   atomic.NewInt64(0),
		sendEvents: atomic.NewInt64(0),
		uploadParts: struct {
			originPartUploadResponse *s3.CreateMultipartUploadOutput
			byteSize                 int64
			completeParts            []*s3.CompletedPart
		}{originPartUploadResponse: nil,
			byteSize:      0,
			completeParts: make([]*s3.CompletedPart, 0, 128),
		},
	}
}

type s3Sink struct {
	storage *storage.S3Storage

	logMeta *logMeta
	encoder func() codec.EventBatchEncoder

	hashMap      sync.Map
	tableBuffers []*tableBuffer
	notifyChan   chan struct{}
}

func (s *s3Sink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	shouldFlush := false
	for _, row := range rows {
		// dispatch row event by tableID
		tableID := row.Table.GetTableID()
		var (
			ok   bool
			item interface{}
			hash int
		)
		if item, ok = s.hashMap.Load(tableID); !ok {
			// found new tableID
			s.tableBuffers = append(s.tableBuffers, newTableBuffer(tableID))
			hash = len(s.tableBuffers) - 1
			s.hashMap.Store(tableID, hash)
		} else {
			hash = item.(int)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.tableBuffers[hash].dataCh <- row:
			s.tableBuffers[hash].sendSize.Add(row.ApproximateSize)
			if s.tableBuffers[hash].sendSize.Load() > maxNotifySize {
				// trigger flush when a table has maxNotifySize
				shouldFlush = true
			}
			s.tableBuffers[hash].sendEvents.Inc()
		}
	}
	if shouldFlush {
		// should not block here
		select {
		case s.notifyChan <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *s3Sink) flushLogMeta(ctx context.Context) error {
	data, err := s.logMeta.Marshal()
	if err != nil {
		return errors.Annotate(err, "marshal meta to json failed")
	}
	return s.storage.Write(ctx, logMetaFile, data)
}

func (s *s3Sink) flushTableBuffers(ctx context.Context) error {
	// TODO use a fixed worker pool
	eg, ectx := errgroup.WithContext(ctx)
	for _, tb := range s.tableBuffers {
		tbReplica := tb
		eg.Go(func() error {
			log.Info("[FlushRowChangedEvents] flush specify row changed event",
				zap.Int64("table", tbReplica.tableID),
				zap.Int64("event size", tbReplica.sendEvents.Load()))
			return tbReplica.flush(ectx, s)
		})
	}
	return eg.Wait()
}

func (s *s3Sink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	// we should flush all events before resolvedTs, there are two kind of flush policy
	// 1. flush row events to a s3 chunk: if the event size is not enough,
	//    TODO: when cdc crashed, we should repair these chunks to a complete file
	// 2. flush row events to a complete s3 file: if the event size is enough
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.notifyChan:
		return s.flushTableBuffers(ctx)

	case <-time.After(defaultFlushRowChangedEventDuration):
		// cannot accumulate enough row events in 10 second
		// flush all tables' row events to s3
		return s.flushTableBuffers(ctx)
	}
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
		s.logMeta.Names[ddl.TableInfo.TableID] = model.QuoteSchema(ddl.TableInfo.Schema, ddl.TableInfo.Table)
		err := s.flushLogMeta(ctx)
		if err != nil {
			return err
		}
	case parsemodel.ActionRenameTable:
		delete(s.logMeta.Names, ddl.PreTableInfo.TableID)
		s.logMeta.Names[ddl.TableInfo.TableID] = model.QuoteSchema(ddl.TableInfo.Schema, ddl.TableInfo.Table)
		err := s.flushLogMeta(ctx)
		if err != nil {
			return err
		}
	}
	encoder := s.encoder()
	_, err := encoder.AppendDDLEvent(ddl)
	if err != nil {
		return err
	}
	data := encoder.MixedBuild()

	listing, err := s.storage.ListObject(ctx, ddlEventsDir, 1)
	if err != nil {
		return err
	}
	var name string
	size := int64(0)
	listPrefix := *listing.Prefix
	for _, content := range listing.Contents {
		name = strings.ReplaceAll(*content.Key, listPrefix, "")
		size = *content.Size
		log.Debug("[EmitDDLEvent] list content from s3",
			zap.String("key", *content.Key),
			zap.String("name", name),
			zap.String("prefix", listPrefix),
			zap.Int64("size", size),
			zap.Any("ddl", ddl))
	}
	var fileData []byte
	if size == 0 || size > maxDDLFlushSize {
		// no ddl file exists or
		// exists file is oversized. we should generate a new file
		fileData = data
		name = makeDDLFileObject(ddl.CommitTs)
		log.Debug("[EmitDDLEvent] create first or rotate ddl log",
			zap.String("name", name), zap.Any("ddl", ddl))
	} else {
		// hack way: append data to old file
		log.Debug("[EmitDDLEvent] append ddl to origin log",
			zap.String("name", name), zap.Any("ddl", ddl))
		data, err := s.storage.Read(ctx, name)
		if err != nil {
			return err
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
					return errors.Annotate(err, "create table directory on s3 failed")
				}
			}
		}
		// update log meta to record the relationship about tableName and tableID
		s.logMeta = makeLogMetaContent(tableInfo)

		data, err := s.logMeta.Marshal()
		if err != nil {
			return errors.Annotate(err, "marshal meta to json failed")
		}
		return s.storage.Write(ctx, logMetaFile, data)
	}
	return nil
}

func (s *s3Sink) Close() error {
	return nil
}

// NewS3Sink creates new sink support log data to s3 directly
func NewS3Sink(sinkURI *url.URL) (*s3Sink, error) {
	if len(sinkURI.Host) == 0 {
		return nil, errors.Errorf("please specify the bucket for s3 in %s", sinkURI)
	}
	prefix := strings.Trim(sinkURI.Path, "/")
	s3 := &backup.S3{Bucket: sinkURI.Host, Prefix: prefix}
	options := &storage.BackendOptions{}
	storage.ExtractQueryParameters(sinkURI, &options.S3)
	if err := options.S3.Apply(s3); err != nil {
		return nil, err
	}
	s3storage, err := storage.NewS3Storage(s3, false)
	if err != nil {
		return nil, err
	}

	notifyChan := make(chan struct{})
	tableBuffers := make([]*tableBuffer, 0)
	return &s3Sink{
		storage: s3storage,
		logMeta: newLogMeta(),
		encoder: codec.NewJSONEventBatchEncoder,

		tableBuffers: tableBuffers,
		notifyChan:   notifyChan,
	}, nil
}

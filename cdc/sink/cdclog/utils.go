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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/log"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
)

const (
	tablePrefix = "t_"
	logMetaFile = "log.meta"

	ddlEventsDir    = "ddls"
	ddlEventsPrefix = "ddl"

	maxUint64 = ^uint64(0)
)

type logUnit interface {
	TableID() int64
	Events() *atomic.Int64
	Size() *atomic.Int64

	dataChan() chan *model.RowChangedEvent

	isEmpty() bool
	shouldFlush() bool
	// flush data to storage.
	flush(ctx context.Context, sink *logSink) error
}

type logSink struct {
	notifyChan     chan []logUnit
	notifyWaitChan chan struct{}

	encoder func() codec.EventBatchEncoder
	units   []logUnit

	// file sink use
	rootPath string
	// s3 sink use
	storagePath storage.ExternalStorage

	hashMap sync.Map
}

func newLogSink(root string, storage storage.ExternalStorage) *logSink {
	return &logSink{
		notifyChan:     make(chan []logUnit),
		notifyWaitChan: make(chan struct{}),
		encoder: func() codec.EventBatchEncoder {
			ret := codec.NewJSONEventBatchEncoder()
			ret.(*codec.JSONEventBatchEncoder).SetMixedBuildSupport(true)
			return ret
		},
		units:       make([]logUnit, 0),
		rootPath:    root,
		storagePath: storage,
	}
}

// s3Sink need this
func (l *logSink) storage() storage.ExternalStorage {
	return l.storagePath
}

// fileSink need this
func (l *logSink) root() string {
	return l.rootPath
}

func (l *logSink) startFlush(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("[startFlush] log sink stopped")
			return ctx.Err()
		case needFlushedUnits := <-l.notifyChan:
			// try specify buffers
			eg, ectx := errgroup.WithContext(ctx)
			for _, u := range needFlushedUnits {
				uReplica := u
				eg.Go(func() error {
					log.Info("start Flush asynchronously to storage by caller",
						zap.Int64("table id", u.TableID()),
						zap.Int64("size", u.Size().Load()),
						zap.Int64("event count", u.Events().Load()),
					)
					return uReplica.flush(ectx, l)
				})
			}
			if err := eg.Wait(); err != nil {
				return err
			}
			// tell flush goroutine this time flush finished
			l.notifyWaitChan <- struct{}{}

		case <-ticker.C:
			// try all tableBuffers
			eg, ectx := errgroup.WithContext(ctx)
			for _, u := range l.units {
				uReplica := u
				if u.shouldFlush() {
					eg.Go(func() error {
						log.Info("start Flush asynchronously to storage",
							zap.Int64("table id", u.TableID()),
							zap.Int64("size", u.Size().Load()),
							zap.Int64("event count", u.Events().Load()),
						)
						return uReplica.flush(ectx, l)
					})
				}
			}
			if err := eg.Wait(); err != nil {
				return err
			}
		}
	}
}

func (l *logSink) emitRowChangedEvents(ctx context.Context, newUnit func(int64) logUnit, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		// dispatch row event by tableID
		tableID := row.Table.GetTableID()
		var (
			ok   bool
			item interface{}
			hash int
		)
		if item, ok = l.hashMap.Load(tableID); !ok {
			// found new tableID
			l.units = append(l.units, newUnit(tableID))
			hash = len(l.units) - 1
			l.hashMap.Store(tableID, hash)
		} else {
			hash = item.(int)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case l.units[hash].dataChan() <- row:
			l.units[hash].Size().Add(row.ApproximateSize)
			l.units[hash].Events().Inc()
		}
	}
	return nil
}

func (l *logSink) flushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	// TODO update flush policy with size
	select {
	case <-ctx.Done():
		return 0, ctx.Err()

	default:
		needFlushedUnits := make([]logUnit, 0, len(l.units))
		for _, u := range l.units {
			if !u.isEmpty() {
				needFlushedUnits = append(needFlushedUnits, u)
			}
		}
		if len(needFlushedUnits) > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()

			case <-time.After(defaultFlushRowChangedEventDuration):
				// cannot accumulate enough row events in 5 second
				// call flushed worker to flush
				l.notifyChan <- needFlushedUnits
				// wait flush worker finished
				<-l.notifyWaitChan
			}
		}
	}
	return resolvedTs, nil

}

type logMeta struct {
	Names            map[int64]string `json:"names"`
	GlobalResolvedTS uint64           `json:"global_resolved_ts"`
}

func newLogMeta() *logMeta {
	return &logMeta{
		Names: make(map[int64]string),
	}
}

// Marshal saves logMeta
func (l *logMeta) Marshal() ([]byte, error) {
	return json.Marshal(l)
}

func makeTableDirectoryName(tableID int64) string {
	return fmt.Sprintf("%s%d", tablePrefix, tableID)
}

func makeTableFileObject(tableID int64, commitTS uint64) string {
	return fmt.Sprintf("%s%d/%s", tablePrefix, tableID, makeTableFileName(commitTS))
}

func makeTableFileName(commitTS uint64) string {
	return fmt.Sprintf("cdclog.%d", commitTS)
}

func makeLogMetaContent(tableInfos []*model.SimpleTableInfo) *logMeta {
	meta := new(logMeta)
	names := make(map[int64]string)
	for _, table := range tableInfos {
		if table != nil {
			log.Info("[makeLogMetaContent]", zap.Reflect("table", table))
			names[table.TableID] = model.QuoteSchema(table.Schema, table.Table)
		}
	}
	meta.Names = names
	return meta
}

func makeDDLFileObject(commitTS uint64) string {
	return fmt.Sprintf("%s/%s", ddlEventsDir, makeDDLFileName(commitTS))
}

func makeDDLFileName(commitTS uint64) string {
	return fmt.Sprintf("%s.%d", ddlEventsPrefix, maxUint64-commitTS)
}

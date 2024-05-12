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

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	cerror "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	ddlsinkfactory "github.com/pingcap/tiflow/cdc/sink/ddlsink/factory"
	eventsinkfactory "github.com/pingcap/tiflow/cdc/sink/dmlsink/factory"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/avro"
	"github.com/pingcap/tiflow/pkg/sink/codec/canal"
	"github.com/pingcap/tiflow/pkg/sink/codec/open"
	"github.com/pingcap/tiflow/pkg/sink/codec/simple"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// NewDecoder will create a new event decoder
func NewDecoder(ctx context.Context, option *consumerOption, upstreamTiDB *sql.DB) (codec.RowEventDecoder, error) {
	var (
		decoder codec.RowEventDecoder
		err     error
	)
	switch option.protocol {
	case config.ProtocolOpen, config.ProtocolDefault:
		decoder, err = open.NewBatchDecoder(ctx, option.codecConfig, upstreamTiDB)
	case config.ProtocolCanalJSON:
		decoder, err = canal.NewBatchDecoder(ctx, option.codecConfig, upstreamTiDB)
		if err != nil {
			return decoder, err
		}
	case config.ProtocolAvro:
		schemaM, err := avro.NewConfluentSchemaManager(ctx, option.schemaRegistryURI, nil)
		if err != nil {
			return decoder, cerror.Trace(err)
		}
		decoder = avro.NewDecoder(option.codecConfig, schemaM, option.topic)
	case config.ProtocolSimple:
		decoder, err = simple.NewDecoder(ctx, option.codecConfig, upstreamTiDB)
	default:
		log.Panic("Protocol not supported", zap.Any("Protocol", option.protocol))
	}
	return decoder, err
}

type writer struct {
	mu       sync.Mutex
	interval time.Duration

	ddlList              []*model.DDLEvent
	ddlListMu            sync.Mutex
	ddlWithMaxCommitTs   *model.DDLEvent
	ddlSink              ddlsink.Sink
	fakeTableIDGenerator *fakeTableIDGenerator

	// sinkFactory is used to create table sink for each table.
	sinkFactory  *eventsinkfactory.SinkFactory
	sinks        []*partitionSinks
	eventsGroups []map[int64]*eventsGroup
	decoders     []codec.RowEventDecoder

	// initialize to 0 by default
	globalResolvedTs uint64

	eventRouter *dispatcher.EventRouter

	upstreamTiDB *sql.DB
}

// NewWriter will create a writer to decode kafka message and write to the downstream.
func NewWriter(ctx context.Context, o *consumerOption) (*writer, error) {
	w := new(writer)

	tz, err := util.GetTimezone(o.timezone)
	if err != nil {
		return nil, cerror.Annotate(err, "can not load timezone")
	}
	config.GetGlobalServerConfig().TZ = o.timezone
	o.codecConfig.TimeZone = tz

	w.fakeTableIDGenerator = &fakeTableIDGenerator{
		tableIDs: make(map[string]int64),
	}

	if o.codecConfig.LargeMessageHandle.HandleKeyOnly() {
		db, err := openDB(ctx, o.upstreamTiDBDSN)
		if err != nil {
			return nil, err
		}
		w.upstreamTiDB = db
	}

	eventRouter, err := dispatcher.NewEventRouter(o.replicaConfig, o.protocol, o.topic, "kafka")
	if err != nil {
		return nil, cerror.Trace(err)
	}
	w.eventRouter = eventRouter

	w.sinks = make([]*partitionSinks, o.partitionNum)
	w.eventsGroups = make([]map[int64]*eventsGroup, o.partitionNum)
	w.decoders = make([]codec.RowEventDecoder, o.partitionNum)
	ctx, cancel := context.WithCancel(ctx)
	errChan := make(chan error, 1)

	for i := 0; i < int(o.partitionNum); i++ {
		w.sinks[i] = &partitionSinks{}
		decoder, err := NewDecoder(ctx, o, w.upstreamTiDB)
		if err != nil {
			log.Panic("Error create decoder", zap.Error(err))
		}
		w.decoders[i] = decoder
		w.eventsGroups[i] = make(map[int64]*eventsGroup)
	}

	changefeedID := model.DefaultChangeFeedID("kafka-consumer")
	f, err := eventsinkfactory.New(ctx, changefeedID, o.downstreamURI, o.replicaConfig, errChan, nil)
	if err != nil {
		cancel()
		return nil, cerror.Trace(err)
	}
	w.sinkFactory = f

	go func() {
		err := <-errChan
		if !errors.Is(cerror.Cause(err), context.Canceled) {
			log.Error("error on running consumer", zap.Error(err))
		} else {
			log.Info("consumer exited")
		}
		cancel()
	}()

	ddlSink, err := ddlsinkfactory.New(ctx, changefeedID, o.downstreamURI, o.replicaConfig)
	if err != nil {
		cancel()
		return nil, cerror.Trace(err)
	}
	w.ddlSink = ddlSink
	w.interval = 100 * time.Millisecond
	return w, nil
}

// append DDL wait to be handled, only consider the constraint among DDLs.
// for DDL a / b received in the order, a.CommitTs < b.CommitTs should be true.
func (w *writer) appendDDL(ddl *model.DDLEvent) {
	w.ddlListMu.Lock()
	defer w.ddlListMu.Unlock()
	// DDL CommitTs fallback, just crash it to indicate the bug.
	if w.ddlWithMaxCommitTs != nil && ddl.CommitTs < w.ddlWithMaxCommitTs.CommitTs {
		log.Warn("DDL CommitTs < maxCommitTsDDL.CommitTs",
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.Uint64("maxCommitTs", w.ddlWithMaxCommitTs.CommitTs),
			zap.String("DDL", ddl.Query))
		return
	}

	// A rename tables DDL job contains multiple DDL events with same CommitTs.
	// So to tell if a DDL is redundant or not, we must check the equivalence of
	// the current DDL and the DDL with max CommitTs.
	if ddl == w.ddlWithMaxCommitTs {
		log.Info("ignore redundant DDL, the DDL is equal to ddlWithMaxCommitTs",
			zap.Uint64("commitTs", ddl.CommitTs), zap.String("DDL", ddl.Query))
		return
	}

	w.ddlList = append(w.ddlList, ddl)
	log.Info("DDL event received", zap.Uint64("commitTs", ddl.CommitTs), zap.String("DDL", ddl.Query))
	w.ddlWithMaxCommitTs = ddl
}

func (w *writer) getFrontDDL() *model.DDLEvent {
	w.ddlListMu.Lock()
	defer w.ddlListMu.Unlock()
	if len(w.ddlList) > 0 {
		return w.ddlList[0]
	}
	return nil
}

func (w *writer) popDDL() {
	w.ddlListMu.Lock()
	defer w.ddlListMu.Unlock()
	if len(w.ddlList) > 0 {
		w.ddlList = w.ddlList[1:]
	}
}

func (w *writer) forEachSink(fn func(sink *partitionSinks) error) error {
	for _, sink := range w.sinks {
		if err := fn(sink); err != nil {
			return cerror.Trace(err)
		}
	}
	return nil
}

func (w *writer) getMinPartitionResolvedTs() (result uint64, err error) {
	result = uint64(math.MaxUint64)
	err = w.forEachSink(func(sink *partitionSinks) error {
		a := atomic.LoadUint64(&sink.resolvedTs)
		if a < result {
			result = a
		}
		return nil
	})
	return result, err
}

// Write will write data downstream synchronously.
func (w *writer) Write(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	minPartitionResolvedTs, err := w.getMinPartitionResolvedTs()
	if err != nil {
		return cerror.Trace(err)
	}
	for {
		// handle DDL
		if todoDDL := w.getFrontDDL(); todoDDL != nil && todoDDL.CommitTs <= minPartitionResolvedTs {
			// flush DMLs
			if err := w.forEachSink(func(sink *partitionSinks) error {
				return syncFlushRowChangedEvents(ctx, sink, todoDDL.CommitTs)
			}); err != nil {
				return cerror.Trace(err)
			}
			// DDL can be executed, do it first.
			if err := w.ddlSink.WriteDDLEvent(ctx, todoDDL); err != nil {
				return cerror.Trace(err)
			}
			w.popDDL()
			if todoDDL.CommitTs < minPartitionResolvedTs {
				log.Info("update minPartitionResolvedTs by DDL",
					zap.Uint64("minPartitionResolvedTs", minPartitionResolvedTs),
					zap.String("DDL", todoDDL.Query))
				minPartitionResolvedTs = todoDDL.CommitTs
			}
		} else {
			break
		}
	}

	// update global resolved ts
	if w.globalResolvedTs > minPartitionResolvedTs {
		log.Panic("global ResolvedTs fallback",
			zap.Uint64("globalResolvedTs", w.globalResolvedTs),
			zap.Uint64("minPartitionResolvedTs", minPartitionResolvedTs))
	}

	if w.globalResolvedTs < minPartitionResolvedTs {
		w.globalResolvedTs = minPartitionResolvedTs
	}

	if err := w.forEachSink(func(sink *partitionSinks) error {
		return syncFlushRowChangedEvents(ctx, sink, w.globalResolvedTs)
	}); err != nil {
		return cerror.Trace(err)
	}
	return nil
}

// Decode is to decode kafka message to event.
func (w *writer) Decode(ctx context.Context, option *consumerOption, partition int32, key []byte, value []byte) error {
	sink := w.sinks[partition]
	decoder := w.decoders[partition]
	eventGroups := w.eventsGroups[partition]

	if err := decoder.AddKeyValue(key, value); err != nil {
		log.Error("add key value to the decoder failed", zap.Error(err))
		return cerror.Trace(err)
	}

	counter := 0
	for {
		tp, hasNext, err := decoder.HasNext()
		if err != nil {
			log.Panic("decode message key failed", zap.Error(err))
		}
		if !hasNext {
			break
		}

		counter++
		// If the message containing only one event exceeds the length limit, CDC will allow it and issue a warning.
		if len(key)+len(value) > option.maxMessageBytes && counter > 1 {
			log.Panic("kafka max-messages-bytes exceeded",
				zap.Int("max-message-bytes", option.maxMessageBytes),
				zap.Int("receivedBytes", len(key)+len(value)))
		}

		switch tp {
		case model.MessageTypeDDL:
			// for some protocol, DDL would be dispatched to all partitions,
			// Consider that DDL a, b, c received from partition-0, the latest DDL is c,
			// if we receive `a` from partition-1, which would be seemed as DDL regression,
			// then cause the consumer panic, but it was a duplicate one.
			// so we only handle DDL received from partition-0 should be enough.
			// but all DDL event messages should be consumed.
			ddl, err := decoder.NextDDLEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.ByteString("value", value),
					zap.Error(err))
			}

			if simple, ok := decoder.(*simple.Decoder); ok {
				cachedEvents := simple.GetCachedEvents()
				for _, row := range cachedEvents {
					var partitionID int64
					if row.TableInfo.IsPartitionTable() {
						partitionID = row.PhysicalTableID
					}
					tableID := w.fakeTableIDGenerator.
						generateFakeTableID(row.TableInfo.GetSchemaName(), row.TableInfo.GetTableName(), partitionID)
					row.TableInfo.TableName.TableID = tableID

					group, ok := eventGroups[tableID]
					if !ok {
						group = NewEventsGroup()
						eventGroups[tableID] = group
					}
					group.Append(row)
				}
			}

			// the Query maybe empty if using simple protocol, it's comes from `bootstrap` event.
			if partition == 0 && ddl.Query != "" {
				w.appendDDL(ddl)
			}
		case model.MessageTypeRow:
			row, err := decoder.NextRowChangedEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.ByteString("value", value),
					zap.Error(err))
			}
			// when using simple protocol, the row may be nil, since it's table info not received yet,
			// it's cached in the decoder, so just continue here.
			if row == nil {
				continue
			}
			target, _, err := w.eventRouter.GetPartitionForRowChange(row, option.partitionNum)
			if err != nil {
				return cerror.Trace(err)
			}
			if partition != target {
				log.Panic("RowChangedEvent dispatched to wrong partition",
					zap.Int32("obtained", partition),
					zap.Int32("expected", target),
					zap.Int32("partitionNum", option.partitionNum),
					zap.Any("row", row),
				)
			}

			globalResolvedTs := atomic.LoadUint64(&w.globalResolvedTs)
			partitionResolvedTs := atomic.LoadUint64(&sink.resolvedTs)
			if row.CommitTs <= globalResolvedTs || row.CommitTs <= partitionResolvedTs {
				log.Warn("RowChangedEvent fallback row, ignore it",
					zap.Uint64("commitTs", row.CommitTs),
					zap.Uint64("globalResolvedTs", globalResolvedTs),
					zap.Uint64("partitionResolvedTs", partitionResolvedTs),
					zap.Int32("partition", partition),
					zap.Any("row", row))
				continue
			}
			var partitionID int64
			if row.TableInfo.IsPartitionTable() {
				partitionID = row.PhysicalTableID
			}
			tableID := w.fakeTableIDGenerator.
				generateFakeTableID(row.TableInfo.GetSchemaName(), row.TableInfo.GetTableName(), partitionID)
			row.TableInfo.TableName.TableID = tableID

			group, ok := eventGroups[tableID]
			if !ok {
				group = NewEventsGroup()
				eventGroups[tableID] = group
			}

			group.Append(row)
		case model.MessageTypeResolved:
			ts, err := decoder.NextResolvedEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.ByteString("value", value),
					zap.Error(err))
			}

			globalResolvedTs := atomic.LoadUint64(&w.globalResolvedTs)
			partitionResolvedTs := atomic.LoadUint64(&sink.resolvedTs)
			if ts < globalResolvedTs || ts < partitionResolvedTs {
				log.Warn("partition resolved ts fallback, skip it",
					zap.Uint64("ts", ts),
					zap.Uint64("partitionResolvedTs", partitionResolvedTs),
					zap.Uint64("globalResolvedTs", globalResolvedTs),
					zap.Int32("partition", partition))
				continue
			}

			for tableID, group := range eventGroups {
				events := group.Resolve(ts)
				if len(events) == 0 {
					continue
				}
				if _, ok := sink.tableSinksMap.Load(tableID); !ok {
					sink.tableSinksMap.Store(tableID, w.sinkFactory.CreateTableSinkForConsumer(
						model.DefaultChangeFeedID("kafka-consumer"),
						spanz.TableIDToComparableSpan(tableID),
						events[0].CommitTs,
					))
				}
				s, _ := sink.tableSinksMap.Load(tableID)
				s.(tablesink.TableSink).AppendRowChangedEvents(events...)
				commitTs := events[len(events)-1].CommitTs
				lastCommitTs, ok := sink.tablesCommitTsMap.Load(tableID)
				if !ok || lastCommitTs.(uint64) < commitTs {
					sink.tablesCommitTsMap.Store(tableID, commitTs)
				}
			}
			atomic.StoreUint64(&sink.resolvedTs, ts)
		}
		// write in time
		if err := w.Write(ctx); err != nil {
			log.Panic("Error write to the downstream", zap.Error(err))
		}
	}

	if counter > option.maxBatchSize {
		log.Panic("Open Protocol max-batch-size exceeded", zap.Int("max-batch-size", option.maxBatchSize),
			zap.Int("actual-batch-size", counter))
	}
	if err := w.Write(ctx); err != nil {
		log.Panic("Error write to the downstream", zap.Error(err))
	}
	return nil
}

type fakeTableIDGenerator struct {
	tableIDs       map[string]int64
	currentTableID int64
	mu             sync.Mutex
}

func (g *fakeTableIDGenerator) generateFakeTableID(schema, table string, partition int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := quotes.QuoteSchema(schema, table)
	if partition != 0 {
		key = fmt.Sprintf("%s.`%d`", key, partition)
	}
	if tableID, ok := g.tableIDs[key]; ok {
		return tableID
	}
	g.currentTableID++
	g.tableIDs[key] = g.currentTableID
	return g.currentTableID
}

// partitionSinks maintained for each partition, it may sync data for multiple tables.
type partitionSinks struct {
	tablesCommitTsMap sync.Map
	tableSinksMap     sync.Map
	// resolvedTs record the maximum timestamp of the received event
	resolvedTs uint64
}

func syncFlushRowChangedEvents(ctx context.Context, sink *partitionSinks, resolvedTs uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		flushedResolvedTs := true
		sink.tablesCommitTsMap.Range(func(key, value interface{}) bool {
			tableID := key.(int64)
			resolvedTs := model.NewResolvedTs(resolvedTs)
			tableSink, ok := sink.tableSinksMap.Load(tableID)
			if !ok {
				log.Panic("Table sink not found", zap.Int64("tableID", tableID))
			}
			if err := tableSink.(tablesink.TableSink).UpdateResolvedTs(resolvedTs); err != nil {
				log.Error("Failed to update resolved ts", zap.Error(err))
				return false
			}
			checkpoint := tableSink.(tablesink.TableSink).GetCheckpointTs()
			if !checkpoint.EqualOrGreater(resolvedTs) {
				flushedResolvedTs = false
			}
			return true
		})
		if flushedResolvedTs {
			return nil
		}
	}
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Error("open db failed", zap.Error(err))
		return nil, cerror.Trace(err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(10 * time.Minute)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		log.Error("ping db failed", zap.String("dsn", dsn), zap.Error(err))
		return nil, cerror.Trace(err)
	}
	log.Info("open db success", zap.String("dsn", dsn))
	return db, nil
}

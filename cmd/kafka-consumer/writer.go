// Copyright 2024 PingCAP, Inc.
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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	ddlsinkfactory "github.com/pingcap/tiflow/cdc/sink/ddlsink/factory"
	eventsinkfactory "github.com/pingcap/tiflow/cdc/sink/dmlsink/factory"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/avro"
	"github.com/pingcap/tiflow/pkg/sink/codec/canal"
	"github.com/pingcap/tiflow/pkg/sink/codec/open"
	"github.com/pingcap/tiflow/pkg/sink/codec/simple"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

// NewDecoder will create a new event decoder
func NewDecoder(ctx context.Context, option *option, upstreamTiDB *sql.DB) (codec.RowEventDecoder, error) {
	var (
		decoder codec.RowEventDecoder
		err     error
	)
	switch option.protocol {
	case config.ProtocolOpen, config.ProtocolDefault:
		decoder, err = open.NewBatchDecoder(ctx, option.codecConfig, upstreamTiDB)
	case config.ProtocolCanalJSON:
		decoder, err = canal.NewBatchDecoder(ctx, option.codecConfig, upstreamTiDB)
	case config.ProtocolAvro:
		schemaM, err := avro.NewConfluentSchemaManager(ctx, option.schemaRegistryURI, nil)
		if err != nil {
			return decoder, cerror.Trace(err)
		}
		decoder = avro.NewDecoder(option.codecConfig, schemaM, option.topic, upstreamTiDB)
	case config.ProtocolSimple:
		decoder, err = simple.NewDecoder(ctx, option.codecConfig, upstreamTiDB)
	default:
		log.Panic("Protocol not supported", zap.Any("Protocol", option.protocol))
	}
	if err != nil {
		return nil, cerror.Trace(err)
	}
	return decoder, err
}

type partitionProgress struct {
	watermark       uint64
	watermarkOffset kafka.Offset
	// tableSinkMap -> [tableID]tableSink
	tableSinkMap sync.Map

	eventGroups map[int64]*eventsGroup
	decoder     codec.RowEventDecoder

	previousMap map[model.TableID]previous

	eventCount int64
	resolvedTs uint64
}

type previous struct {
	offset   kafka.Offset
	commitTs uint64
}

type writer struct {
	option *option

	ddlList              []*model.DDLEvent
	ddlWithMaxCommitTs   *model.DDLEvent
	ddlSink              ddlsink.Sink
	fakeTableIDGenerator *fakeTableIDGenerator

	// sinkFactory is used to create table sink for each table.
	sinkFactory *eventsinkfactory.SinkFactory
	progresses  []*partitionProgress

	eventRouter *dispatcher.EventRouter
}

func newWriter(ctx context.Context, o *option) *writer {
	w := &writer{
		option: o,
		fakeTableIDGenerator: &fakeTableIDGenerator{
			tableIDs: make(map[string]int64),
		},
		progresses: make([]*partitionProgress, o.partitionNum),
	}

	eventRouter, err := dispatcher.NewEventRouter(o.replicaConfig, o.protocol, o.topic, "kafka")
	if err != nil {
		log.Panic("initialize the event router failed",
			zap.Any("protocol", o.protocol), zap.Any("topic", o.topic),
			zap.Any("dispatcherRules", o.replicaConfig.Sink.DispatchRules), zap.Error(err))
	}
	w.eventRouter = eventRouter
	log.Info("event router created", zap.Any("protocol", o.protocol),
		zap.Any("topic", o.topic), zap.Any("dispatcherRules", o.replicaConfig.Sink.DispatchRules))

	var db *sql.DB

	if o.upstreamTiDBDSN != "" {
		db, err = openDB(ctx, o.upstreamTiDBDSN)
		if err != nil {
			log.Panic("cannot open the upstream TiDB, handle key only enabled",
				zap.String("dsn", o.upstreamTiDBDSN))
		}
	}

	for i := 0; i < int(o.partitionNum); i++ {
		decoder, err := NewDecoder(ctx, o, db)
		if err != nil {
			log.Panic("cannot create the decoder", zap.Error(err))
		}
		w.progresses[i] = &partitionProgress{
			eventGroups: make(map[int64]*eventsGroup),
			previousMap: make(map[model.TableID]previous),
			decoder:     decoder,
		}
	}

	config.GetGlobalServerConfig().TZ = o.timezone
	errChan := make(chan error, 1)
	changefeed := model.DefaultChangeFeedID("kafka-consumer")
	f, err := eventsinkfactory.New(ctx, changefeed, o.downstreamURI, o.replicaConfig, errChan, nil)
	if err != nil {
		log.Panic("cannot create the event sink factory", zap.Error(err))
	}
	w.sinkFactory = f

	go func() {
		err := <-errChan
		if !errors.Is(cerror.Cause(err), context.Canceled) {
			log.Error("error on running consumer", zap.Error(err))
		} else {
			log.Info("consumer exited")
		}
	}()

	ddlSink, err := ddlsinkfactory.New(ctx, changefeed, o.downstreamURI, o.replicaConfig)
	if err != nil {
		log.Panic("cannot create the ddl sink factory", zap.Error(err))
	}
	w.ddlSink = ddlSink
	return w
}

// append DDL wait to be handled, only consider the constraint among DDLs.
// for DDL a / b received in the order, a.CommitTs < b.CommitTs should be true.
func (w *writer) appendDDL(ddl *model.DDLEvent) {
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
		log.Warn("ignore redundant DDL, the DDL is equal to ddlWithMaxCommitTs",
			zap.Uint64("commitTs", ddl.CommitTs), zap.String("DDL", ddl.Query))
		return
	}

	w.ddlList = append(w.ddlList, ddl)
	w.ddlWithMaxCommitTs = ddl
}

func (w *writer) getFrontDDL() *model.DDLEvent {
	if len(w.ddlList) > 0 {
		return w.ddlList[0]
	}
	return nil
}

func (w *writer) popDDL() {
	if len(w.ddlList) > 0 {
		w.ddlList = w.ddlList[1:]
	}
}

func (w *writer) getMinWatermark() uint64 {
	result := uint64(math.MaxUint64)
	for _, p := range w.progresses {
		watermark := atomic.LoadUint64(&p.watermark)
		if watermark < result {
			result = watermark
		}
	}
	return result
}

// partition progress could be executed at the same time
func (w *writer) forEachPartition(fn func(p *partitionProgress)) {
	var wg sync.WaitGroup
	for _, p := range w.progresses {
		wg.Add(1)
		go func(p *partitionProgress) {
			defer wg.Done()
			fn(p)
		}(p)
	}
	wg.Wait()
}

// Write will synchronously write data downstream
func (w *writer) Write(ctx context.Context, messageType model.MessageType) bool {
	watermark := w.getMinWatermark()
	var todoDDL *model.DDLEvent
	for {
		todoDDL = w.getFrontDDL()
		// watermark is the min value for all partitions,
		// the DDL only executed by the first partition, other partitions may be slow
		// so that the watermark can be smaller than the DDL's commitTs,
		// which means some DML events may not be consumed yet, so cannot execute the DDL right now.
		if todoDDL == nil || todoDDL.CommitTs > watermark {
			break
		}
		// flush DMLs
		w.forEachPartition(func(sink *partitionProgress) {
			syncFlushRowChangedEvents(ctx, sink, todoDDL.CommitTs)
		})
		// DDL can be executed, do it first.
		if err := w.ddlSink.WriteDDLEvent(ctx, todoDDL); err != nil {
			log.Panic("write DDL event failed", zap.Error(err),
				zap.String("DDL", todoDDL.Query), zap.Uint64("commitTs", todoDDL.CommitTs))
		}
		w.popDDL()
	}

	if messageType == model.MessageTypeResolved {
		w.forEachPartition(func(sink *partitionProgress) {
			syncFlushRowChangedEvents(ctx, sink, watermark)
		})
	}

	// The DDL events will only execute in partition0
	if messageType == model.MessageTypeDDL && todoDDL != nil {
		log.Info("DDL event will be flushed in the future",
			zap.Uint64("watermark", watermark),
			zap.Uint64("CommitTs", todoDDL.CommitTs),
			zap.String("Query", todoDDL.Query))
		return false
	}
	return true
}

// WriteMessage is to decode kafka message to event.
func (w *writer) WriteMessage(ctx context.Context, message *kafka.Message) bool {
	var (
		key       = message.Key
		value     = message.Value
		partition = message.TopicPartition.Partition
	)

	progress := w.progresses[partition]
	decoder := progress.decoder
	// eventGroup := progress.eventGroups
	if err := decoder.AddKeyValue(key, value); err != nil {
		log.Panic("add key value to the decoder failed",
			zap.Int32("partition", partition), zap.Any("offset", message.TopicPartition.Offset),
			zap.Error(err))
	}
	var (
		counter     int
		needFlush   bool
		messageType model.MessageType
	)
	for {
		ty, hasNext, err := decoder.HasNext()
		if err != nil {
			log.Panic("decode message key failed",
				zap.Int32("partition", partition), zap.Any("offset", message.TopicPartition.Offset),
				zap.Error(err))
		}
		if !hasNext {
			break
		}
		counter++
		// If the message containing only one event exceeds the length limit, CDC will allow it and issue a warning.
		if len(key)+len(value) > w.option.maxMessageBytes && counter > 1 {
			log.Panic("kafka max-messages-bytes exceeded",
				zap.Int32("partition", partition), zap.Any("offset", message.TopicPartition.Offset),
				zap.Int("max-message-bytes", w.option.maxMessageBytes),
				zap.Int("receivedBytes", len(key)+len(value)))
		}
		messageType = ty
		switch messageType {
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
					zap.Int32("partition", partition), zap.Any("offset", message.TopicPartition.Offset),
					zap.ByteString("value", value),
					zap.Error(err))
			}

			// if simple, ok := decoder.(*simple.Decoder); ok {
			// 	cachedEvents := simple.GetCachedEvents()
			// 	for _, row := range cachedEvents {
			// 		row.TableInfo.TableName.TableID = row.PhysicalTableID
			// 		w.checkPartition(row, partition, message)
			// 		if w.checkOldMessage(progress, row.CommitTs, row, partition, message) {
			// 			continue
			// 		}
			// 		group, ok := eventGroup[row.PhysicalTableID]
			// 		if !ok {
			// 			group = NewEventsGroup()
			// 			eventGroup[row.PhysicalTableID] = group
			// 		}
			// 		group.Append(row)
			// 	}
			// }

			// the Query maybe empty if using simple protocol, it's comes from `bootstrap` event.
			if partition == 0 && ddl.Query != "" {
				w.appendDDL(ddl)
				needFlush = true
				log.Info("DDL message received",
					zap.Int32("partition", partition),
					zap.Any("offset", message.TopicPartition.Offset),
					zap.Uint64("commitTs", ddl.CommitTs),
					zap.String("DDL", ddl.Query))
			}
		case model.MessageTypeRow:
			row, err := decoder.NextRowChangedEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.Int32("partition", partition), zap.Any("offset", message.TopicPartition.Offset),
					zap.ByteString("value", value),
					zap.Error(err))
			}
			// when using simple protocol, the row may be nil, since it's table info not received yet,
			// it's cached in the decoder, so just continue here.
			if w.option.protocol == config.ProtocolSimple && row == nil {
				continue
			}

			tableID := row.PhysicalTableID
			// simple protocol decoder should have set the table id already.
			if w.option.protocol != config.ProtocolSimple {
				tableID = w.fakeTableIDGenerator.
					generateFakeTableID(row.TableInfo.GetSchemaName(), row.TableInfo.GetTableName(), row.PhysicalTableID)
				row.TableInfo.TableName.TableID = tableID
			}

			w.checkPartition(row, partition, message)

			if w.checkOldMessage(progress, row.CommitTs, row, partition, message) {
				continue
			}

			prev, ok := progress.previousMap[tableID]
			if ok {
				if prev.commitTs > row.CommitTs && prev.offset < message.TopicPartition.Offset {
					watermark := atomic.LoadUint64(&progress.watermark)
					if row.CommitTs < atomic.LoadUint64(&progress.watermark) {
						log.Panic("row changed event commitTs fallback",
							zap.Int64("tableID", tableID), zap.Int32("partition", partition), zap.Uint64("watermark", watermark),
							zap.Uint64("previous", prev.commitTs), zap.Uint64("commitTs", row.CommitTs),
							zap.Any("previousOffset", prev.offset), zap.Any("offset", message.TopicPartition.Offset))
					}
					log.Warn("row changed event commitTs fallback, ignore it",
						zap.Int64("tableID", tableID), zap.Int32("partition", partition), zap.Uint64("watermark", watermark),
						zap.Uint64("previous", prev.commitTs), zap.Uint64("commitTs", row.CommitTs),
						zap.Any("previousOffset", prev.offset), zap.Any("offset", message.TopicPartition.Offset))
				}
			}
			progress.previousMap[tableID] = previous{
				offset:   message.TopicPartition.Offset,
				commitTs: row.CommitTs,
			}

			// group, ok := eventGroup[tableID]
			// if !ok {
			// 	group = NewEventsGroup()
			// 	eventGroup[tableID] = group
			// }
			// group.Append(row)
			log.Info("DML event received",
				zap.String("schema", row.TableInfo.GetSchemaName()),
				zap.String("table", row.TableInfo.GetTableName()),
				zap.Int32("partition", partition),
				zap.Any("offset", message.TopicPartition.Offset),
				zap.Uint64("commitTs", row.CommitTs))

			resolvedTs := max(progress.resolvedTs, row.CommitTs)
			progress.resolvedTs = resolvedTs
			tableSink, ok := progress.tableSinkMap.Load(tableID)
			if !ok {
				tableSink = w.sinkFactory.CreateTableSinkForConsumer(
					model.DefaultChangeFeedID("kafka-consumer"),
					spanz.TableIDToComparableSpan(tableID),
					row.CommitTs,
				)
				progress.tableSinkMap.Store(tableID, tableSink)
			}
			tableSink.(tablesink.TableSink).AppendRowChangedEvents(row)
			progress.eventCount++
			if progress.eventCount >= 1000 {
				// the max commitTs of all tables
				atomic.StoreUint64(&progress.watermark, resolvedTs)
				progress.watermarkOffset = message.TopicPartition.Offset
				needFlush = true
				log.Info("flush row changed events",
					zap.Int32("partition", partition), zap.Uint64("resolvedTs", resolvedTs), zap.Int64("count", progress.eventCount))
				progress.eventCount = 0
			}
		case model.MessageTypeResolved:
		default:
			log.Panic("unknown message type", zap.Any("messageType", messageType),
				zap.Int32("partition", partition), zap.Any("offset", message.TopicPartition.Offset))
		}
	}

	if counter > w.option.maxBatchSize {
		log.Panic("Open Protocol max-batch-size exceeded",
			zap.Int("max-batch-size", w.option.maxBatchSize), zap.Int("actual-batch-size", counter),
			zap.Int32("partition", partition), zap.Any("offset", message.TopicPartition.Offset))
	}

	if !needFlush {
		return false
	}
	// flush when received DDL event or resolvedTs
	return w.Write(ctx, messageType)
}

func (w *writer) checkPartition(row *model.RowChangedEvent, partition int32, message *kafka.Message) {
	target, _, err := w.eventRouter.GetPartitionForRowChange(row, w.option.partitionNum)
	if err != nil {
		log.Panic("cannot calculate partition for the row changed event",
			zap.Int32("partition", partition), zap.Any("offset", message.TopicPartition.Offset),
			zap.Int32("partitionNum", w.option.partitionNum), zap.Int64("tableID", row.TableInfo.TableName.TableID),
			zap.Error(err), zap.Any("event", row))
	}
	if partition != target {
		log.Panic("RowChangedEvent dispatched to wrong partition",
			zap.Int32("partition", partition), zap.Int32("expected", target),
			zap.Int32("partitionNum", w.option.partitionNum),
			zap.Any("offset", message.TopicPartition.Offset),
			zap.Int64("tableID", row.TableInfo.TableName.TableID), zap.Any("row", row),
		)
	}
}

func (w *writer) checkOldMessage(progress *partitionProgress, ts uint64, row *model.RowChangedEvent, partition int32, message *kafka.Message) bool {
	watermark := atomic.LoadUint64(&progress.watermark)
	if row == nil {
		watermark := atomic.LoadUint64(&progress.watermark)
		if ts < watermark {
			if message.TopicPartition.Offset > progress.watermarkOffset {
				log.Panic("partition resolved ts fallback, skip it",
					zap.Uint64("ts", ts), zap.Any("offset", message.TopicPartition.Offset),
					zap.Uint64("watermark", watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
					zap.Int32("partition", partition))
			}
			log.Warn("partition resolved ts fall back, ignore it, since consumer read old offset message",
				zap.Uint64("ts", ts), zap.Any("offset", message.TopicPartition.Offset),
				zap.Uint64("watermark", watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
				zap.Int32("partition", partition))
			return true
		}
		return false
	}
	// if the kafka cluster is normal, this should not hit.
	// else if the cluster is abnormal, the consumer may consume old message, then cause the watermark fallback.
	if ts < watermark {
		// if commit message failed, the consumer may read previous message,
		// just ignore this message should be fine, otherwise panic.
		if message.TopicPartition.Offset > progress.watermarkOffset {
			log.Panic("RowChangedEvent fallback row",
				zap.Uint64("commitTs", ts), zap.Any("offset", message.TopicPartition.Offset),
				zap.Uint64("watermark", watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
				zap.Int32("partition", partition), zap.Int64("tableID", row.TableInfo.TableName.TableID),
				zap.String("schema", row.TableInfo.GetSchemaName()),
				zap.String("table", row.TableInfo.GetTableName()))
		}
		log.Warn("Row changed event fall back, ignore it, since consumer read old offset message",
			zap.Uint64("commitTs", row.CommitTs), zap.Any("offset", message.TopicPartition.Offset),
			zap.Uint64("watermark", watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
			zap.Int32("partition", partition), zap.Int64("tableID", row.TableInfo.TableName.TableID),
			zap.String("schema", row.TableInfo.GetSchemaName()),
			zap.String("table", row.TableInfo.GetTableName()))
		return true
	}
	return false
}

type fakeTableIDGenerator struct {
	tableIDs       map[string]int64
	currentTableID int64
}

func (g *fakeTableIDGenerator) generateFakeTableID(schema, table string, partition int64) int64 {
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

func syncFlushRowChangedEvents(ctx context.Context, progress *partitionProgress, watermark uint64) {
	for {
		select {
		case <-ctx.Done():
			log.Warn("sync flush row changed event canceled", zap.Error(ctx.Err()))
			return
		default:
		}
		flushedResolvedTs := true
		progress.tableSinkMap.Range(func(key, value interface{}) bool {
			resolvedTs := model.NewResolvedTs(watermark)
			tableSink := value.(tablesink.TableSink)
			// todo: can we update resolved ts for each table sink concurrently ?
			// this maybe helpful to accelerate the consume process, and reduce the memory usage.
			if err := tableSink.UpdateResolvedTs(resolvedTs); err != nil {
				log.Panic("Failed to update resolved ts", zap.Error(err))
			}
			if tableSink.GetCheckpointTs().Less(resolvedTs) {
				flushedResolvedTs = false
			}
			return true
		})
		if flushedResolvedTs {
			return
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

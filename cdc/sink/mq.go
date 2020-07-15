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

package sink

import (
	"context"

	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	"github.com/pingcap/ticdc/cdc/sink/dispatcher"
	"github.com/pingcap/ticdc/cdc/sink/mqProducer"
	"github.com/pingcap/ticdc/cdc/sink/pulsar"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type mqSink struct {
	mqProducer mqProducer.Producer
	dispatcher dispatcher.Dispatcher
	newEncoder func() codec.EventBatchEncoder
	filter     *filter.Filter
	protocol   codec.Protocol

	partitionNum   int32
	partitionInput []chan struct {
		row        *model.RowChangedEvent
		resolvedTs uint64
	}
	partitionResolvedTs []uint64
	checkpointTs        uint64
	resolvedNotifier    *notify.Notifier
	resolvedReceiver    *notify.Receiver

	statistics *Statistics
}

func newMqSink(ctx context.Context, mqProducer mqProducer.Producer, filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
	partitionNum := mqProducer.GetPartitionNum()
	partitionInput := make([]chan struct {
		row        *model.RowChangedEvent
		resolvedTs uint64
	}, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		partitionInput[i] = make(chan struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}, 12800)
	}
	d, err := dispatcher.NewDispatcher(config, mqProducer.GetPartitionNum())
	if err != nil {
		return nil, errors.Trace(err)
	}
	notifier := new(notify.Notifier)
	var protocol codec.Protocol
	protocol.FromString(config.Sink.Protocol)

	newEncoder := codec.NewEventBatchEncoder(protocol)
	if protocol == codec.ProtocolAvro {
		registryURI, ok := opts["registry"]
		if !ok {
			return nil, errors.New(`Avro protocol requires parameter "registry"`)
		}
		schemaManager, err := codec.NewAvroSchemaManager(ctx, registryURI, "-value")
		if err != nil {
			return nil, errors.Annotate(err, "Could not create Avro schema manager")
		}
		newEncoder1 := newEncoder
		newEncoder = func() codec.EventBatchEncoder {
			avroEncoder := newEncoder1().(*codec.AvroEventBatchEncoder)
			avroEncoder.SetValueSchemaManager(schemaManager)
			return avroEncoder
		}
	}

	k := &mqSink{
		mqProducer: mqProducer,
		dispatcher: d,
		newEncoder: newEncoder,
		filter:     filter,
		protocol:   protocol,

		partitionNum:        partitionNum,
		partitionInput:      partitionInput,
		partitionResolvedTs: make([]uint64, partitionNum),
		resolvedNotifier:    notifier,
		resolvedReceiver:    notifier.NewReceiver(50 * time.Millisecond),

		statistics: NewStatistics("MQ", opts),
	}

	go func() {
		if err := k.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			}
		}
	}()
	return k, nil
}

func (k *mqSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		if k.filter.ShouldIgnoreDMLEvent(row.StartTs, row.Table.Schema, row.Table.Table) {
			log.Info("Row changed event ignored", zap.Uint64("start-ts", row.StartTs))
			continue
		}
		partition := k.dispatcher.Dispatch(row)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.partitionInput[partition] <- struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}{row: row}:
		}
	}
	return nil
}

func (k *mqSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	if resolvedTs <= k.checkpointTs {
		return nil
	}

	for i := 0; i < int(k.partitionNum); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.partitionInput[i] <- struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}{resolvedTs: resolvedTs}:
		}
	}

	// waiting for all row events are sent to mq producer
flushLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-k.resolvedReceiver.C:
			for i := 0; i < int(k.partitionNum); i++ {
				if resolvedTs > atomic.LoadUint64(&k.partitionResolvedTs[i]) {
					continue flushLoop
				}
			}
			break flushLoop
		}
	}
	err := k.mqProducer.Flush(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	k.checkpointTs = resolvedTs
	k.statistics.PrintStatus()
	return nil
}

func (k *mqSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	switch k.protocol {
	case codec.ProtocolAvro: // ignore resolved events in avro protocol
	case codec.ProtocolCanal:
		return nil

	}
	encoder := k.newEncoder()
	err := encoder.AppendResolvedEvent(ts)
	if err != nil {
		return errors.Trace(err)
	}
	key, value := encoder.Build()
	err = k.mqProducer.SyncBroadcastMessage(ctx, key, value)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(err)
}

func (k *mqSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	if k.filter.ShouldIgnoreDDLEvent(ddl.StartTs, ddl.Schema, ddl.Table) {
		log.Info(
			"DDL event ignored",
			zap.String("query", ddl.Query),
			zap.Uint64("startTs", ddl.StartTs),
			zap.Uint64("commitTs", ddl.CommitTs),
		)
		return errors.Trace(model.ErrorDDLEventIgnored)
	}
	encoder := k.newEncoder()
	err := encoder.AppendDDLEvent(ddl)
	if err != nil {
		return errors.Trace(err)
	}

	if k.protocol != codec.ProtocolAvro {
		key, value := encoder.Build()
		log.Info("emit ddl event", zap.ByteString("key", key), zap.ByteString("value", value))
		err = k.mqProducer.SyncBroadcastMessage(ctx, key, value)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Initialize registers Avro schemas for all tables
func (k *mqSink) Initialize(ctx context.Context, tableInfo []*model.TableInfo) error {
	if k.protocol == codec.ProtocolAvro && tableInfo != nil {
		avroEncoder := k.newEncoder().(*codec.AvroEventBatchEncoder)
		manager := avroEncoder.GetValueSchemaManager()
		if manager == nil {
			return errors.New("No schema manager in Avro encoder, probably bug")
		}

		for _, info := range tableInfo {
			if info == nil {
				continue
			}

			if k.filter.ShouldIgnoreTable(info.Schema, info.Table) {
				log.Info("Skip creating schema for table", zap.String("table-name", info.Table))
				continue
			}

			str, err := codec.ColumnInfoToAvroSchema(info.Table, info.ColumnInfo)
			if err != nil {
				return errors.Annotate(err, "Error in Initialize")
			}

			avroCodec, err := goavro.NewCodec(str)
			if err != nil {
				return errors.Annotate(err, "Initialize failed: could not verify schema, probably bug")
			}

			err = manager.Register(context.Background(), model.TableName{
				Schema: info.Schema,
				Table:  info.Table,
			}, avroCodec)

			if err != nil {
				return errors.Annotate(err, "Initialize failed: could not register schema")
			}
		}
	}
	return nil
}

func (k *mqSink) Close() error {
	err := k.mqProducer.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (k *mqSink) run(ctx context.Context) error {
	defer k.resolvedReceiver.Stop()
	wg, ctx := errgroup.WithContext(ctx)
	for i := int32(0); i < k.partitionNum; i++ {
		partition := i
		wg.Go(func() error {
			return k.runWorker(ctx, partition)
		})
	}
	return wg.Wait()
}

const batchSizeLimit = 4 * 1024 * 1024 // 4MB

func (k *mqSink) runWorker(ctx context.Context, partition int32) error {
	input := k.partitionInput[partition]
	encoder := k.newEncoder()
	batchSize := 0
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	flushToProducer := func() error {
		return k.statistics.RecordBatchExecution(func() (int, error) {
			if batchSize == 0 {
				return 0, nil
			}
			key, value := encoder.Build()
			encoder = k.newEncoder()
			thisBatchSize := batchSize
			batchSize = 0
			return thisBatchSize, k.mqProducer.SendMessage(ctx, key, value, partition)
		})
	}
	for {
		var e struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if err := flushToProducer(); err != nil {
				return errors.Trace(err)
			}
			continue
		case e = <-input:
		}
		if e.row == nil {
			if e.resolvedTs != 0 {
				if err := flushToProducer(); err != nil {
					return errors.Trace(err)
				}
				atomic.StoreUint64(&k.partitionResolvedTs[partition], e.resolvedTs)
				k.resolvedNotifier.Notify()
			}
			continue
		}
		err := encoder.AppendRowChangedEvent(e.row)
		if err != nil {
			return errors.Trace(err)
		}
		batchSize++
		if encoder.Size() >= batchSizeLimit {
			if err := flushToProducer(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func newKafkaSaramaSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter, replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
	config := mqProducer.DefaultKafkaConfig

	scheme := strings.ToLower(sinkURI.Scheme)
	if scheme != "kafka" {
		return nil, errors.New("can not create MQ sink with unsupported scheme")
	}
	s := sinkURI.Query().Get("partition-num")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		config.PartitionNum = int32(c)
	}

	s = sinkURI.Query().Get("replication-factor")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		config.ReplicationFactor = int16(c)
	}

	s = sinkURI.Query().Get("kafka-version")
	if s != "" {
		config.Version = s
	}

	s = sinkURI.Query().Get("max-message-bytes")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		config.MaxMessageBytes = c
	}

	s = sinkURI.Query().Get("compression")
	if s != "" {
		config.Compression = s
	}

	config.ClientID = sinkURI.Query().Get("kafka-client-id")

	s = sinkURI.Query().Get("protocol")
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}

	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	producer, err := mqProducer.NewKafkaSaramaProducer(ctx, sinkURI.Host, topic, config, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sink, err := newMqSink(ctx, producer, filter, replicaConfig, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}

func newPulsarSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter, replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
	producer, err := pulsar.NewProducer(sinkURI, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s := sinkURI.Query().Get("protocol")
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}
	sink, err := newMqSink(ctx, producer, filter, replicaConfig, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}

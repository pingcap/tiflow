package sink

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	"github.com/pingcap/ticdc/cdc/sink/dispatcher"
	"github.com/pingcap/ticdc/cdc/sink/mqProducer"
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

func newMqSink(ctx context.Context, mqProducer mqProducer.Producer, filter *filter.Filter, config *filter.ReplicaConfig, opts map[string]string, errCh chan error) *mqSink {
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
	notifier := new(notify.Notifier)
	k := &mqSink{
		mqProducer: mqProducer,
		dispatcher: dispatcher.NewDispatcher(config, mqProducer.GetPartitionNum()),
		newEncoder: codec.NewJSONEventBatchEncoder,
		filter:     filter,

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
	return k
}

func (k *mqSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		if k.filter.ShouldIgnoreEvent(row.Ts, row.Schema, row.Table) {
			log.Info("Row changed event ignored", zap.Uint64("ts", row.Ts))
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
	return nil
}

func (k *mqSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
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
	if k.filter.ShouldIgnoreEvent(ddl.Ts, ddl.Schema, ddl.Table) {
		log.Info(
			"DDL event ignored",
			zap.String("query", ddl.Query),
			zap.Uint64("ts", ddl.Ts),
		)
		return nil
	}
	encoder := k.newEncoder()
	err := encoder.AppendDDLEvent(ddl)
	if err != nil {
		return errors.Trace(err)
	}
	key, value := encoder.Build()
	log.Info("emit ddl event", zap.ByteString("key", key), zap.ByteString("value", value))
	err = k.mqProducer.SyncBroadcastMessage(ctx, key, value)
	if err != nil {
		return errors.Trace(err)
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

func newKafkaSaramaSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter, replicaConfig *filter.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
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

	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	producer, err := mqProducer.NewKafkaSaramaProducer(ctx, sinkURI.Host, topic, config, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newMqSink(ctx, producer, filter, replicaConfig, opts, errCh), nil
}

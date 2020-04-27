package sink

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/cdc/sink/dispatcher"

	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/pkg/util"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/mqProducer"
)

type mqSink struct {
	mqProducer   mqProducer.Producer
	partitionNum int32

	globalResolvedTs uint64
	checkpointTs     uint64
	filter           *util.Filter
	dispatcher       dispatcher.Dispatcher

	captureID    string
	changefeedID string

	errCh chan error
}

func newMqSink(mqProducer mqProducer.Producer, filter *util.Filter, config *util.ReplicaConfig, opts map[string]string) *mqSink {
	return &mqSink{
		mqProducer:   mqProducer,
		partitionNum: mqProducer.GetPartitionNum(),
		dispatcher:   dispatcher.NewDispatcher(config, mqProducer.GetPartitionNum()),
		filter:       filter,
		changefeedID: opts[OptChangefeedID],
		captureID:    opts[OptCaptureID],
		errCh:        make(chan error, 1),
	}
}

func (k *mqSink) EmitResolvedEvent(ctx context.Context, ts uint64) error {
	atomic.StoreUint64(&k.globalResolvedTs, ts)
	return nil
}

func (k *mqSink) EmitCheckpointEvent(ctx context.Context, ts uint64) error {
	err := k.mqProducer.SyncBroadcastMessage(ctx, model.NewResolvedMessage(ts), nil)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (k *mqSink) EmitRowChangedEvent(ctx context.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		if row.Resolved {
			err := k.mqProducer.SendMessage(ctx, model.NewResolvedMessage(row.Ts), nil, 0)
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if k.filter.ShouldIgnoreEvent(row.Ts, row.Schema, row.Table) {
			log.Info("Row changed event ignored", zap.Uint64("ts", row.Ts))
			continue
		}
		partition := k.dispatcher.Dispatch(row)
		key, value := row.ToMqMessage()
		err := k.mqProducer.SendMessage(ctx, key, value, partition)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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
	key, value := ddl.ToMqMessage()
	log.Info("emit ddl event", zap.Reflect("key", key), zap.Reflect("value", value))
	err := k.mqProducer.SyncBroadcastMessage(ctx, key, value)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (k *mqSink) CheckpointTs() uint64 {
	return atomic.LoadUint64(&k.checkpointTs)
}

func (k *mqSink) Count() uint64 {
	return k.mqProducer.Count()
}

func (k *mqSink) Run(ctx context.Context) error {
	wg, cctx := errgroup.WithContext(ctx)
	if !util.IsOwnerFromCtx(ctx) {
		wg.Go(func() error {
			return k.run(cctx)
		})
		wg.Go(func() error {
			return k.collectMetrics(ctx)
		})
	}
	wg.Go(func() error {
		return k.mqProducer.Run(cctx)
	})
	return wg.Wait()
}

func (k *mqSink) collectMetrics(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(defaultMetricInterval):
			mqSinkCheckpointChanSizeGauge.WithLabelValues(k.captureID, k.changefeedID).Set(float64(len(k.mqProducer.Successes())))
		}
	}
}

func (k *mqSink) run(ctx context.Context) error {
	for {
		var sinkCheckpoint uint64
		select {
		case <-ctx.Done():
			if err := k.Close(); err != nil {
				log.Error("close mq sink failed", zap.Error(err))
			}
			return ctx.Err()
		case err := <-k.errCh:
			log.Error("found err in MQ Sink, exiting", zap.Error(err))
			if err := k.Close(); err != nil {
				log.Error("close mq sink failed", zap.Error(err))
			}
			return err
		case sinkCheckpoint = <-k.mqProducer.Successes():
		}

		globalResolvedTs := atomic.LoadUint64(&k.globalResolvedTs)
		// when local resolvedTS is fallback, we will postpone to pushing global resolvedTS
		// check if the global resolvedTS is postponed

		if globalResolvedTs < sinkCheckpoint {
			sinkCheckpoint = globalResolvedTs
		}
		atomic.StoreUint64(&k.checkpointTs, sinkCheckpoint)
	}
}

func (k *mqSink) Close() error {
	err := k.mqProducer.Close()
	if err != nil {
		return errors.Trace(err)
	}
	close(k.errCh)
	return nil
}

func (k *mqSink) PrintStatus(ctx context.Context) error {
	return k.mqProducer.PrintStatus(ctx)
}

func newKafkaSaramaSink(ctx context.Context, sinkURI *url.URL, filter *util.Filter, replicaConfig *util.ReplicaConfig, opts map[string]string) (*mqSink, error) {
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
	producer, err := mqProducer.NewKafkaSaramaProducer(ctx, sinkURI.Host, topic, config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newMqSink(producer, filter, replicaConfig, opts), nil
}

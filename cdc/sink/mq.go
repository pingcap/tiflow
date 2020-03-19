package sink

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/pkg/util"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/mqProducer"
)

type mqSink struct {
	mqProducer       mqProducer.Producer
	partitionNum     int32
	lastSentMsgIndex uint64

	sinkCheckpointTsCh chan struct {
		ts    uint64
		index uint64
	}
	globalResolvedTs uint64
	checkpointTs     uint64
	filter           *util.Filter

	changefeedID string

	count int64

	errCh chan error
}

func newMqSink(mqProducer mqProducer.Producer, filter *util.Filter, opts map[string]string) *mqSink {
	partitionNum := mqProducer.GetPartitionNum()
	changefeedID := opts[OptChangefeedID]
	return &mqSink{
		mqProducer:   mqProducer,
		partitionNum: partitionNum,
		sinkCheckpointTsCh: make(chan struct {
			ts    uint64
			index uint64
		}, 128),
		filter:       filter,
		changefeedID: changefeedID,
		errCh:        make(chan error, 1),
	}
}

func (k *mqSink) EmitResolvedEvent(ctx context.Context, ts uint64) error {
	atomic.StoreUint64(&k.globalResolvedTs, ts)
	return nil
}

func (k *mqSink) EmitCheckpointEvent(ctx context.Context, ts uint64) error {
	keyByte, err := model.NewResolvedMessage(ts).Encode()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("owner emit cpoint ts", zap.ByteString("b", keyByte))
	err = k.mqProducer.SyncBroadcastMessage(ctx, keyByte, nil)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (k *mqSink) EmitRowChangedEvent(ctx context.Context, rows ...*model.RowChangedEvent) error {
	var sinkCheckpointTs uint64
	for _, row := range rows {
		if row.Resolved {
			sinkCheckpointTs = row.Ts
			continue
		}
		if k.filter.ShouldIgnoreEvent(row.Ts, row.Schema, row.Table) {
			log.Info("Row changed event ignored", zap.Uint64("ts", row.Ts))
			continue
		}
		partition := k.calPartition(row)
		key, value := row.ToMqMessage()
		keyByte, err := key.Encode()
		if err != nil {
			return errors.Trace(err)
		}
		valueByte, err := value.Encode()
		if err != nil {
			return errors.Trace(err)
		}
		k.lastSentMsgIndex, err = k.mqProducer.SendMessage(ctx, keyByte, valueByte, partition, func(err error) {
			if err != nil {
				log.Error("failed to send row changed event to kafka", zap.Error(err), zap.Reflect("row", row))
				return
			}
			atomic.AddInt64(&k.count, 1)
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
	if sinkCheckpointTs == 0 {
		return nil
	}
	// handle sink checkpoint ts
	select {
	case <-ctx.Done():
		return ctx.Err()
	case k.sinkCheckpointTsCh <- struct {
		ts    uint64
		index uint64
	}{ts: sinkCheckpointTs, index: k.lastSentMsgIndex}:
	}
	return nil
}

func (k *mqSink) calPartition(row *model.RowChangedEvent) int32 {
	hash := crc32.NewIEEE()
	// distribute partition by table
	_, err := hash.Write([]byte(row.Schema))
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}
	_, err = hash.Write([]byte(row.Table))
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}

	if len(row.IndieMarkCol) > 0 {
		// distribute partition by rowid or unique column value
		value := row.Columns[row.IndieMarkCol].Value
		b, err := json.Marshal(value)
		if err != nil {
			log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
		}
		_, err = hash.Write(b)
		if err != nil {
			log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
		}
	}
	return int32(hash.Sum32() % uint32(k.partitionNum))
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
	keyByte, err := key.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	valueByte, err := value.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("owner start finished SyncBroadcastMessage")
	err = k.mqProducer.SyncBroadcastMessage(ctx, keyByte, valueByte)
	log.Info("finished SyncBroadcastMessage")
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (k *mqSink) CheckpointTs() uint64 {
	return atomic.LoadUint64(&k.checkpointTs)
}

func (k *mqSink) Run(ctx context.Context) error {
	wg, cctx := errgroup.WithContext(ctx)
	log.Info("IsOwner", zap.Bool("b", util.IsOwnerFromCtx(ctx)))
	if !util.IsOwnerFromCtx(ctx) {
		wg.Go(func() error {
			return k.run(cctx)
		})
	}
	wg.Go(func() error {
		return k.mqProducer.Run(cctx)
	})
	return wg.Wait()
}

func (k *mqSink) run(ctx context.Context) error {
	closeSink := func() {
		err := k.mqProducer.Close()
		if err != nil {
			log.Error("close MQ Producer failed", zap.Error(err))
		}
		close(k.errCh)
	}
	for {
		var sinkCheckpoint struct {
			ts    uint64
			index uint64
		}
		select {
		case <-ctx.Done():
			closeSink()
			return ctx.Err()
		case err := <-k.errCh:
			closeSink()
			return err
		case sinkCheckpoint = <-k.sinkCheckpointTsCh:
		}

		// wait mq producer send message successfully
		for sinkCheckpoint.index > k.mqProducer.MaxSuccessesIndex() {
			log.Info("wait index", zap.Uint64("sinkCheckpoint.index", sinkCheckpoint.index))
			time.Sleep(20 * time.Millisecond)
		}
		globalResolvedTs := atomic.LoadUint64(&k.globalResolvedTs)
		log.Info("global resolved", zap.Uint64("globalResolved", globalResolvedTs))
		// when local resolvedTS is fallback, we will postpone to pushing global resolvedTS
		// check if the global resolvedTS is postponed

		log.Info("sinkCheckpoint.ts", zap.Uint64("sinkCheckpoint.ts", sinkCheckpoint.ts))
		if globalResolvedTs < sinkCheckpoint.ts {
			sinkCheckpoint.ts = globalResolvedTs
		}
		atomic.StoreUint64(&k.checkpointTs, sinkCheckpoint.ts)
	}
}

func (k *mqSink) PrintStatus(ctx context.Context) error {
	lastTime := time.Now()
	var lastCount int64
	timer := time.NewTicker(printStatusInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			now := time.Now()
			seconds := now.Unix() - lastTime.Unix()
			total := atomic.LoadInt64(&k.count)
			count := total - lastCount
			qps := int64(0)
			if seconds > 0 {
				qps = count / seconds
			}
			lastCount = total
			lastTime = now
			log.Info("MQ sink replication status",
				zap.String("changefeed", k.changefeedID),
				zap.Int64("count", count),
				zap.Int64("qps", qps))
		}
	}
}

func newKafkaSaramaSink(ctx context.Context, sinkURI *url.URL, filter *util.Filter, opts map[string]string) (*mqSink, error) {
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

	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	producer, err := mqProducer.NewKafkaSaramaProducer(ctx, sinkURI.Host, topic, config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newMqSink(producer, filter, opts), nil
}

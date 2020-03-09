package sink

import (
	"context"
	"hash/crc32"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/mqProducer"
)

type mqSink struct {
	mqProducer   mqProducer.Producer
	partitionNum int32

	sinkCheckpointTsCh chan uint64
	globalResolvedTs   uint64
	checkpointTs       uint64
}

func newMqSink(mqProducer mqProducer.Producer) *mqSink {
	partitionNum := mqProducer.GetPartitionNum()
	return &mqSink{
		mqProducer:         mqProducer,
		partitionNum:       partitionNum,
		sinkCheckpointTsCh: make(chan uint64, 128),
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
	err = k.mqProducer.BroadcastMessage(ctx, keyByte, nil)
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
		err = k.mqProducer.SendMessage(ctx, keyByte, valueByte, partition)
		if err != nil {
			log.Error("send message failed", zap.ByteStrings("row", [][]byte{keyByte, valueByte}), zap.Int32("partition", partition))
			return errors.Trace(err)
		}
	}
	if sinkCheckpointTs == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case k.sinkCheckpointTsCh <- sinkCheckpointTs:
	}
	return nil
}

func (k *mqSink) calPartition(row *model.RowChangedEvent) int32 {
	hash := crc32.NewIEEE()
	_, err := hash.Write([]byte(row.Schema))
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}
	_, err = hash.Write([]byte(row.Table))
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}

	return int32(hash.Sum32() % uint32(k.partitionNum))
}

func (k *mqSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	key, value := ddl.ToMqMessage()
	keyByte, err := key.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	valueByte, err := value.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	err = k.mqProducer.BroadcastMessage(ctx, keyByte, valueByte)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (k *mqSink) CheckpointTs() uint64 {
	return atomic.LoadUint64(&k.checkpointTs)
}

func (k *mqSink) Run(ctx context.Context) error {
	for {
		var sinkCheckpointTs uint64
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sinkCheckpointTs = <-k.sinkCheckpointTsCh:
		}
		globalResolvedTs := atomic.LoadUint64(&k.globalResolvedTs)
		// when local resolvedTS is fallback, we will postpone to pushing global resolvedTS
		// check if the global resolvedTS is postponed
		if globalResolvedTs < sinkCheckpointTs {
			sinkCheckpointTs = globalResolvedTs
		}
		atomic.StoreUint64(&k.checkpointTs, sinkCheckpointTs)
	}
}

func (k *mqSink) PrintStatus(ctx context.Context) error {
	// TODO implement this function
	<-ctx.Done()
	return nil
}

func (k *mqSink) Close() error {
	return nil
}

func newKafkaSaramaSink(sinkURI *url.URL) (*mqSink, error) {
	config := mqProducer.DefaultKafkaConfig

	scheme := strings.ToLower(sinkURI.Scheme)
	if scheme != "kafka" {
		return nil, errors.New("can not create MQ sink with unsupported scheme")
	}
	s := sinkURI.Query().Get("partition-num")
	if s == "" {
		return nil, errors.New("partition-num can not be empty")
	}
	c, err := strconv.Atoi(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	partitionNum := int32(c)
	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	producer, err := mqProducer.NewKafkaSaramaProducer(sinkURI.Host, topic, partitionNum, config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newMqSink(producer), nil
}

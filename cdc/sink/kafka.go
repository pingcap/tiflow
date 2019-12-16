package sink

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

type kafkaSink struct {
	topic     string
	producer  sarama.SyncProducer
	partition int32
	cdcId     string

	infoGetter TableInfoGetter
}

type KafkaConfig struct {
	topic            string
	KafkaVersion     string
	KafkaAddrs       string
	KafkaMaxMessages int
}

var (
	_ Sink = &kafkaSink{}
)

func NewKafkaSink(cdcId string, topic string, partition int32, producer sarama.SyncProducer, infoGetter TableInfoGetter) (*kafkaSink, error) {
	log.Info("Create kafka sink", zap.Int32("partition", partition))
	return &kafkaSink{
		producer:   producer,
		partition:  partition,
		topic:      topic,
		infoGetter: infoGetter,
		cdcId:      cdcId,
	}, nil

}

func (s *kafkaSink) Emit(ctx context.Context, t model.Txn) error {
	filterBySchemaAndTable(&t)
	if len(t.DMLs) == 0 && t.DDL == nil {
		log.Info("Whole txn ignored", zap.Uint64("ts", t.Ts))
		return nil
	}

	data, err := NewTxnWriter(s.cdcId, t, s.infoGetter).Write()
	if err != nil {
		return errors.Trace(err)
	}

	msg := &sarama.ProducerMessage{Topic: s.topic, Key: nil, Value: sarama.ByteEncoder(data), Partition: s.partition}
	_, _, err = s.producer.SendMessage(msg)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *kafkaSink) EmitResolvedTimestamp(ctx context.Context, resolved uint64) error {
	data, err := NewResloveTsWriter(s.cdcId, resolved).Write()
	if err != nil {
		return errors.Trace(err)
	}

	msg := &sarama.ProducerMessage{Topic: s.topic, Key: nil, Value: sarama.ByteEncoder(data), Partition: s.partition}
	_, _, err = s.producer.SendMessage(msg)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *kafkaSink) Flush(ctx context.Context) error {
	return nil
}

func (s *kafkaSink) Close() error {
	return nil
}

func (s *kafkaSink) sendMsg(data []byte) error {
	msg := &sarama.ProducerMessage{Topic: s.topic, Key: nil, Value: sarama.ByteEncoder(data)}
	_, _, err := s.producer.SendMessage(msg)
	return err
}

// NewSaramaConfig return the default config and set the according version and metrics
func newSaramaConfig(kafkaVersion string) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.ClientID = "tidb_cdc"
	config.Version = version
	log.Debug("kafka consumer", zap.Stringer("version", version))

	return config, nil
}

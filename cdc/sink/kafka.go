package sink

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
	"strings"
	"time"
)

type kafkaSink struct {
	topic string
	producer sarama.SyncProducer

	tblInspector tableInspector
	infoGetter   TableInfoGetter
}

type KafkaConfig struct {
	topic string
	KafkaVersion string
	KafkaAddrs string
	KafkaMaxMessages int
}

var (
	_ Sink = &kafkaSink{}
)

func NewKafkaSink(cfg KafkaConfig) (*kafkaSink, error) {
	config, err := newSaramaConfig(cfg.KafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.Producer.Flush.MaxMessages = cfg.KafkaMaxMessages
	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond


	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.MaxMessageBytes = 1 << 30
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.Retry.Max = 10000
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	producer, err :=  sarama.NewSyncProducer(strings.Split(cfg.KafkaAddrs, ","), config)
	if err != nil {

	}


}

func (s *kafkaSink) Emit(ctx context.Context, t model.Txn) error {
	filterBySchemaAndTable(&t)
	if len(t.DMLs) == 0 && t.DDL == nil {
		log.Info("Whole txn ignored", zap.Uint64("ts", t.Ts))
		return nil
	}



	msg := &sarama.ProducerMessage{Topic: s.topic, Key: nil, Value: sarama.ByteEncoder(data)}

}

func (s *kafkaSink) EmitResolvedTimestamp(ctx context.Context, resolved uint64) error {
	return nil
}

func (s *kafkaSink) Flush(ctx context.Context) error {
	return nil
}

func (s *kafkaSink) Close() error {
	return nil
}

func (s *kafkaSink) sendMsg(data []byte) error{
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

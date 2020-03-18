package mqProducer

import (
	"context"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// KafkaConfig stores the Kafka configuration
type KafkaConfig struct {
	PartitionNum      int32
	ReplicationFactor int16

	Version         string
	MaxMessageBytes int
}

// DefaultKafkaConfig is the default Kafka configuration
var DefaultKafkaConfig = KafkaConfig{
	Version:           "2.4.0",
	MaxMessageBytes:   1 << 26, // 64M
	ReplicationFactor: 1,
}

type kafkaSaramaProducer struct {
	client       sarama.SyncProducer
	topic        string
	partitionNum int32
}

// NewKafkaSaramaProducer creates a kafka sarama producer
func NewKafkaSaramaProducer(address string, topic string, config KafkaConfig) (*kafkaSaramaProducer, error) {
	cfg, err := newSaramaConfig(config)
	if err != nil {
		return nil, err
	}
	client, err := sarama.NewSyncProducer(strings.Split(address, ","), cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// get partition number or create topic automatically
	admin, err := sarama.NewClusterAdmin(strings.Split(address, ","), cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return nil, errors.Trace(err)
	}
	partitionNum := config.PartitionNum
	topicDetail, exist := topics[topic]
	if exist {
		log.Info("get partition number of topic", zap.String("topic", topic), zap.Int32("partition_num", topicDetail.NumPartitions))
		if partitionNum == 0 {
			partitionNum = topicDetail.NumPartitions
		} else if partitionNum < topicDetail.NumPartitions {
			log.Warn("partition number assigned in sink-uri is less than that of topic")
		} else if partitionNum > topicDetail.NumPartitions {
			return nil, errors.Errorf("partition number(%d) assigned in sink-uri is more than that of topic(%d)", partitionNum, topicDetail.NumPartitions)
		}
	} else {
		if partitionNum == 0 {
			partitionNum = 4
			log.Warn("topic not found and partition number is not specified, using default partition number", zap.String("topic", topic), zap.Int32("partition_num", partitionNum))
		}
		err := admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     partitionNum,
			ReplicationFactor: config.ReplicationFactor,
		}, false)
		log.Info("create a topic", zap.String("topic", topic), zap.Int32("partition_num", partitionNum), zap.Int16("replication_factor", config.ReplicationFactor))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	err = admin.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kafkaSaramaProducer{
		client:       client,
		topic:        topic,
		partitionNum: partitionNum,
	}, nil
}

// NewSaramaConfig return the default config and set the according version and metrics
func newSaramaConfig(c KafkaConfig) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.ClientID = "ticdc_kafka_sarama_producer"
	config.Version = version
	log.Debug("kafka consumer", zap.Stringer("version", version))

	config.Producer.Flush.MaxMessages = c.MaxMessageBytes
	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = 1 << 30
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.Retry.Max = 10000
	config.Producer.Retry.Backoff = 500 * time.Millisecond
	return config, err
}

func (k *kafkaSaramaProducer) SendMessage(ctx context.Context, key []byte, value []byte, partition int32) error {
	_, _, err := k.client.SendMessage(&sarama.ProducerMessage{
		Topic:     k.topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Partition: partition,
	})
	return errors.Trace(err)
}

func (k *kafkaSaramaProducer) BroadcastMessage(ctx context.Context, key []byte, value []byte) error {
	for i := int32(0); i < k.partitionNum; i++ {
		err := k.SendMessage(ctx, key, value, i)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (k *kafkaSaramaProducer) GetPartitionNum() int32 {
	return k.partitionNum
}

func (k *kafkaSaramaProducer) Close() error {
	return k.client.Close()
}

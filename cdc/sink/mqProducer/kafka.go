package mqProducer

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/cdc/model"

	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/pkg/util"

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
	Compression     string
}

// DefaultKafkaConfig is the default Kafka configuration
var DefaultKafkaConfig = KafkaConfig{
	Version:           "2.4.0",
	MaxMessageBytes:   512 * 1024 * 1024, // 512M
	ReplicationFactor: 1,
	Compression:       "none",
}

type kafkaSaramaProducer struct {
	syncClient   sarama.SyncProducer
	asyncClient  sarama.AsyncProducer
	topic        string
	partitionNum int32

	rowPartitionCh []chan kafkaRowMsg
	successes      chan uint64

	count     uint64
	totalSize uint64

	closeCh chan struct{}
}

type kafkaRowMsg struct {
	key   *model.MqMessageKey
	value *model.MqMessageRow
}

func (k *kafkaSaramaProducer) SendMessage(ctx context.Context, key *model.MqMessageKey, value *model.MqMessageRow, partition int32) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case k.rowPartitionCh[int(partition)] <- kafkaRowMsg{
		key: key, value: value,
	}:
	}
	return nil
}

func (k *kafkaSaramaProducer) BroadcastMessage(ctx context.Context, key *model.MqMessageKey, value *model.MqMessageDDL) error {
	panic("implement me")
}

func (k *kafkaSaramaProducer) PrintStatus(ctx context.Context) error {
	lastTime := time.Now()
	var lastCount uint64
	var lastSize uint64
	timer := time.NewTicker(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			now := time.Now()
			seconds := uint64(now.Unix() - lastTime.Unix())
			total := atomic.LoadUint64(&k.count)
			totalSize := atomic.LoadUint64(&k.totalSize)
			count := total - lastCount
			countSize := totalSize - lastSize
			qps := uint64(0)
			speed := uint64(0)
			var speedMB float64
			if seconds > 0 {
				qps = count / seconds
				speed = countSize / seconds
				speedMB = float64(speed) / float64(1024*1024)
			}

			lastCount = total
			lastSize = totalSize
			lastTime = now
			log.Info("MQ sink replication status",
				zap.Uint64("count", count),
				zap.Uint64("qps", qps), zap.Float64("speed(MB/S)", speedMB))
		}
	}
}

func (k *kafkaSaramaProducer) SyncBroadcastMessage(ctx context.Context, key *model.MqMessageKey, value *model.MqMessageDDL) error {

	keyByte, err := key.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	var valueByte []byte
	if value != nil {
		valueByte, err = value.Encode()
		if err != nil {
			return errors.Trace(err)
		}
	}
	for partition := int32(0); partition < k.partitionNum; partition++ {
		_, _, err := k.syncClient.SendMessage(&sarama.ProducerMessage{
			Topic:     k.topic,
			Key:       sarama.ByteEncoder(keyByte),
			Value:     sarama.ByteEncoder(valueByte),
			Partition: partition,
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (k *kafkaSaramaProducer) Successes() chan uint64 {
	return k.successes
}

func (k *kafkaSaramaProducer) Run(ctx context.Context) error {

	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case <-k.closeCh:
				return nil
			case msg := <-k.asyncClient.Successes():
				if msg.Metadata != nil {
					checkpointTs := msg.Metadata.(uint64)
					k.successes <- checkpointTs
				}
			case err := <-k.asyncClient.Errors():
				log.Fatal("write kafka error", zap.Error(err))
			}
		}
	})
	errg.Go(func() error {
		return k.runWorker(ctx)
	})
	return errg.Wait()
}

const batchSize = 64 * 1024 //64kb

func (k *kafkaSaramaProducer) runWorker(ctx context.Context) error {
	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	errg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < int(k.partitionNum); i++ {
		partition := i
		rowPartitionCh := k.rowPartitionCh[partition]
		errg.Go(func() error {
			batchKey := []byte{'['}
			batchValue := []byte{'['}
			// TODO 监控
			flush := func(resolved bool, resolvedTs uint64) {
				batchKey = append(batchKey, ']')
				batchValue = append(batchValue, ']')
				msg := &sarama.ProducerMessage{
					Topic:     k.topic,
					Key:       sarama.ByteEncoder(batchKey),
					Value:     sarama.ByteEncoder(batchValue),
					Partition: int32(partition),
				}
				if resolved {
					msg.Metadata = resolvedTs
				}
				select {
				case <-ctx.Done():
					return
				case k.asyncClient.Input() <- msg:
				}
				atomic.AddUint64(&k.count, 1)
				atomic.AddUint64(&k.totalSize, uint64(len(batchValue)+len(batchKey)))
				mqBatchHistogram.WithLabelValues(captureID, changefeedID).
					Observe(float64(len(batchValue) + len(batchKey)))
				batchKey = []byte{'['}
				batchValue = []byte{'['}
			}
			tick := time.NewTicker(500 * time.Millisecond)
			for {
				var msg kafkaRowMsg
				select {
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				case <-tick.C:
					flush(false, 0)
					continue
				case msg = <-rowPartitionCh:
				}
				if msg.key.Type == model.MqMessageTypeResolved {
					// TODO correctness problem
					flush(true, msg.key.Ts)
				}

				keyByte, err := msg.key.Encode()
				if err != nil {
					return errors.Trace(err)
				}
				valueByte, err := msg.value.Encode()
				if err != nil {
					return errors.Trace(err)
				}
				batchKey = append(batchKey, ',')
				batchKey = append(batchKey, keyByte...)
				batchValue = append(batchValue, ',')
				batchValue = append(batchValue, valueByte...)
				if len(batchValue) >= batchSize || len(batchKey) >= batchSize {
					flush(false, 0)
				}
			}
		})
	}
	return errg.Wait()
}

// NewKafkaSaramaProducer creates a kafka sarama producer
func NewKafkaSaramaProducer(ctx context.Context, address string, topic string, config KafkaConfig) (*kafkaSaramaProducer, error) {
	log.Info("Starting kafka sarama producer ...", zap.Reflect("config", config))
	cfg, err := newSaramaConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	cfg.Producer.Return.Errors = true
	syncClient, err := sarama.NewSyncProducer(strings.Split(address, ","), cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cfg, err = newSaramaConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	asyncClient, err := sarama.NewAsyncProducer(strings.Split(address, ","), cfg)
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
		log.Info("create a topic", zap.String("topic", topic), zap.Int32("partition_num", partitionNum), zap.Int16("replication_factor", config.ReplicationFactor))
		err := admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     partitionNum,
			ReplicationFactor: config.ReplicationFactor,
		}, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	err = admin.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}
	rowPartitionCh := make([]chan kafkaRowMsg, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		rowPartitionCh[i] = make(chan kafkaRowMsg, 128000)
	}

	return &kafkaSaramaProducer{
		asyncClient:    asyncClient,
		syncClient:     syncClient,
		topic:          topic,
		partitionNum:   partitionNum,
		closeCh:        make(chan struct{}),
		rowPartitionCh: rowPartitionCh,
		successes:      make(chan uint64, 12800),
	}, nil
}

// NewSaramaConfig return the default config and set the according version and metrics
func newSaramaConfig(ctx context.Context, c KafkaConfig) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var role string
	if util.IsOwnerFromCtx(ctx) {
		role = "owner"
	} else {
		role = "processor"
	}
	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)

	config.ClientID = fmt.Sprintf("TiCDC_sarama_producer_%s_%s_%s", role, captureID, changefeedID)
	config.Version = version
	sarama.MaxRequestSize = int32(c.MaxMessageBytes)
	config.Producer.Flush.MaxMessages = c.MaxMessageBytes
	config.Metadata.Retry.Max = 20
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = c.MaxMessageBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = false
	config.Producer.RequiredAcks = sarama.WaitForAll

	switch strings.ToLower(strings.TrimSpace(c.Compression)) {
	case "none":
		config.Producer.Compression = sarama.CompressionNone
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		log.Warn("Unsupported compression algorithm", zap.String("compression", c.Compression))
		config.Producer.Compression = sarama.CompressionNone
	}

	config.Producer.Retry.Max = 20
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	config.Admin.Retry.Max = 10000
	config.Admin.Retry.Backoff = 500 * time.Millisecond
	config.Admin.Timeout = 20 * time.Second

	return config, err
}

func (k *kafkaSaramaProducer) GetPartitionNum() int32 {
	return k.partitionNum
}

func (k *kafkaSaramaProducer) Close() error {
	close(k.closeCh)
	return k.asyncClient.Close()
}

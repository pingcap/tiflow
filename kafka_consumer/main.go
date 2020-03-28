package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
)

// Sarama configuration options
var (
	kafkaAddrs        []string
	kafkaTopic        string
	kafkaPartitionNum int32
	kafkaGroupID      = fmt.Sprintf("ticdc_kafka_consumer_%s", uuid.New().String())
	kafkaVersion      = "2.4.0"

	downstreamURIStr string

	logPath  string
	logLevel string
)

func init() {
	var upstreamURIStr string

	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "Kafka uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&logPath, "log-file", "cdc_kafka_consumer.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log file path")
	flag.Parse()

	err := util.InitLogger(&util.Config{
		Level: logLevel,
		File:  logPath,
	})
	if err != nil {
		log.Fatal("init logger failed", zap.Error(err))
	}

	upstreamURI, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Fatal("invalid upstream-uri", zap.Error(err))
	}
	scheme := strings.ToLower(upstreamURI.Scheme)
	if scheme != "kafka" {
		log.Fatal("invalid upstream-uri scheme, the scheme of upstream-uri must be `kafka`", zap.String("upstream-uri", upstreamURIStr))
	}
	s := upstreamURI.Query().Get("version")
	if s != "" {
		kafkaVersion = s
	}
	s = upstreamURI.Query().Get("consumer-group-id")
	if s != "" {
		kafkaGroupID = s
	}
	kafkaTopic = strings.TrimFunc(upstreamURI.Path, func(r rune) bool {
		return r == '/'
	})
	kafkaAddrs = strings.Split(upstreamURI.Host, ",")

	config, err := newSaramaConfig()
	if err != nil {
		log.Fatal("Error creating sarama config", zap.Error(err))
	}

	s = upstreamURI.Query().Get("partition-num")
	if s == "" {
		partition, err := getPartitionNum(kafkaAddrs, kafkaTopic, config)
		if err != nil {
			log.Fatal("can not get partition number", zap.String("topic", kafkaTopic), zap.Error(err))
		}
		kafkaPartitionNum = partition
	} else {
		c, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal("invalid partition-num of upstream-uri")
		}
		kafkaPartitionNum = int32(c)
	}
}

func getPartitionNum(address []string, topic string, cfg *sarama.Config) (int32, error) {
	// get partition number or create topic automatically
	admin, err := sarama.NewClusterAdmin(address, cfg)
	if err != nil {
		return 0, errors.Trace(err)
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return 0, errors.Trace(err)
	}
	err = admin.Close()
	if err != nil {
		return 0, errors.Trace(err)
	}
	topicDetail, exist := topics[topic]
	if !exist {
		return 0, errors.Errorf("can not find topic %s", topic)
	}
	log.Info("get partition number of topic", zap.String("topic", topic), zap.Int32("partition_num", topicDetail.NumPartitions))
	return topicDetail.NumPartitions, nil
}

func waitTopicCreated(address []string, topic string, cfg *sarama.Config) error {
	admin, err := sarama.NewClusterAdmin(address, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer admin.Close()
	for i := 0; i <= 30; i++ {
		topics, err := admin.ListTopics()
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := topics[topic]; ok {
			return nil
		}
		log.Info("wait the topic created", zap.String("topic", topic))
		time.Sleep(1 * time.Second)
	}
	return errors.Errorf("wait the topic(%s) created timeout", topic)
}
func newSaramaConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.ClientID = "ticdc_kafka_sarama_consumer"
	config.Version = version

	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	return config, err
}

func main() {
	log.Info("Starting a new TiCDC open protocol consumer")

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config, err := newSaramaConfig()
	if err != nil {
		log.Fatal("Error creating sarama config", zap.Error(err))
	}
	err = waitTopicCreated(kafkaAddrs, kafkaTopic, config)
	if err != nil {
		log.Fatal("wait topic created failed", zap.Error(err))
	}
	/**
	 * Setup a new Sarama consumer group
	 */
	consumer, err := NewConsumer(context.TODO())
	if err != nil {
		log.Fatal("Error creating consumer", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(kafkaAddrs, kafkaGroupID, config)
	if err != nil {
		log.Fatal("Error creating consumer group client", zap.Error(err))
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(kafkaTopic, ","), consumer); err != nil {
				log.Fatal("Error from consumer: %v", zap.Error(err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	go func() {
		if err := consumer.Run(ctx); err != nil {
			log.Fatal("Error running consumer: %v", zap.Error(err))
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Info("TiCDC open protocol consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Info("terminating: context cancelled")
	case <-sigterm:
		log.Info("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Fatal("Error closing client", zap.Error(err))
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool

	ddlList          []*model.DDLEvent
	maxDDLReceivedTs uint64
	ddlListMu        sync.Mutex

	sinks []*struct {
		sink.Sink
		resolvedTs uint64
	}
	sinksMu sync.Mutex

	ddlSink sink.Sink

	globalResolvedTs uint64
}

// NewConsumer creates a new cdc kafka consumer
func NewConsumer(ctx context.Context) (*Consumer, error) {
	// TODO support filter in downstream sink
	filter, err := util.NewFilter(&util.ReplicaConfig{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := new(Consumer)
	c.sinks = make([]*struct {
		sink.Sink
		resolvedTs uint64
	}, kafkaPartitionNum)
	for i := 0; i < int(kafkaPartitionNum); i++ {
		s, err := sink.NewSink(ctx, downstreamURIStr, filter, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		c.sinks[i] = &struct {
			sink.Sink
			resolvedTs uint64
		}{Sink: s}
	}
	sink, err := sink.NewSink(ctx, downstreamURIStr, filter, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.ddlSink = sink
	c.ready = make(chan bool)
	return c, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the c as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := context.TODO()
	partition := claim.Partition()
	c.sinksMu.Lock()
	sink := c.sinks[partition]
	c.sinksMu.Unlock()
	if sink == nil {
		panic("sink should initialized")
	}
	for message := range claim.Messages() {
		log.Debug("Message claimed", zap.Int32("partition", message.Partition), zap.ByteString("key", message.Key), zap.ByteString("value", message.Value))
		key := new(model.MqMessageKey)
		err := key.Decode(message.Key)
		if err != nil {
			log.Fatal("decode message key failed", zap.Error(err))
		}

		switch key.Type {
		case model.MqMessageTypeDDL:
			value := new(model.MqMessageDDL)
			err := value.Decode(message.Value)
			if err != nil {
				log.Fatal("decode message value failed", zap.ByteString("value", message.Value))
			}

			ddl := new(model.DDLEvent)
			ddl.FromMqMessage(key, value)
			c.appendDDL(ddl)
		case model.MqMessageTypeRow:
			globalResolvedTs := atomic.LoadUint64(&c.globalResolvedTs)
			if key.Ts <= globalResolvedTs || key.Ts <= sink.resolvedTs {
				log.Info("filter fallback row", zap.ByteString("row", message.Key),
					zap.Uint64("globalResolvedTs", globalResolvedTs),
					zap.Uint64("sinkResolvedTs", sink.resolvedTs))
				break
			}
			value := new(model.MqMessageRow)
			err := value.Decode(message.Value)
			if err != nil {
				log.Fatal("decode message value failed", zap.ByteString("value", message.Value))
			}
			row := new(model.RowChangedEvent)
			row.FromMqMessage(key, value)
			err = sink.EmitRowChangedEvent(ctx, row)
			if err != nil {
				log.Fatal("emit row changed event failed", zap.Error(err))
			}
		case model.MqMessageTypeResolved:
			err := sink.EmitRowChangedEvent(ctx, &model.RowChangedEvent{Ts: key.Ts, Resolved: true})
			if err != nil {
				log.Fatal("meit row changed event failed", zap.Error(err))
			}
			resolvedTs := atomic.LoadUint64(&sink.resolvedTs)
			if resolvedTs < key.Ts {
				atomic.StoreUint64(&sink.resolvedTs, key.Ts)
			}
		}
		session.MarkMessage(message, "")
	}

	return nil
}

func (c *Consumer) appendDDL(ddl *model.DDLEvent) {
	c.ddlListMu.Lock()
	defer c.ddlListMu.Unlock()
	if ddl.Ts <= c.maxDDLReceivedTs {
		return
	}
	globalResolvedTs := atomic.LoadUint64(&c.globalResolvedTs)
	if ddl.Ts <= globalResolvedTs {
		log.Error("unexpected ddl job", zap.Uint64("ddlts", ddl.Ts), zap.Uint64("globalResolvedTs", globalResolvedTs))
		return
	}
	c.ddlList = append(c.ddlList, ddl)
	c.maxDDLReceivedTs = ddl.Ts
}

func (c *Consumer) getFrontDDL() *model.DDLEvent {
	c.ddlListMu.Lock()
	defer c.ddlListMu.Unlock()
	if len(c.ddlList) > 0 {
		return c.ddlList[0]
	}
	return nil
}

func (c *Consumer) popDDL() *model.DDLEvent {
	c.ddlListMu.Lock()
	defer c.ddlListMu.Unlock()
	if len(c.ddlList) > 0 {
		ddl := c.ddlList[0]
		c.ddlList = c.ddlList[1:]
		return ddl
	}
	return nil
}

func (c *Consumer) forEachSink(fn func(sink *struct {
	sink.Sink
	resolvedTs uint64
}) error) error {
	c.sinksMu.Lock()
	defer c.sinksMu.Unlock()
	for _, sink := range c.sinks {
		if err := fn(sink); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Run runs the Consumer
func (c *Consumer) Run(ctx context.Context) error {
	wg := &sync.WaitGroup{}
	err := c.forEachSink(func(sink *struct {
		sink.Sink
		resolvedTs uint64
	}) error {
		wg.Add(1)
		go func() {
			if err := sink.Run(ctx); err != nil {
				log.Fatal("sink running error", zap.Error(err))
			}
			wg.Done()
		}()
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	var lastGlobalResolvedTs uint64
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		default:
		}
		time.Sleep(100 * time.Millisecond)
		// handle ddl
		globalCheckpointTs := uint64(math.MaxUint64)
		err = c.forEachSink(func(sink *struct {
			sink.Sink
			resolvedTs uint64
		}) error {
			checkpointTs := sink.CheckpointTs()
			if checkpointTs < globalCheckpointTs {
				globalCheckpointTs = checkpointTs
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		todoDDL := c.getFrontDDL()
		if todoDDL != nil && globalCheckpointTs == todoDDL.Ts {
			// execute ddl
			err := c.ddlSink.EmitDDLEvent(ctx, todoDDL)
			if err != nil {
				return errors.Trace(err)
			}
			c.popDDL()
		}

		//handle global resolvedTs
		globalResolvedTs := uint64(math.MaxUint64)
		err = c.forEachSink(func(sink *struct {
			sink.Sink
			resolvedTs uint64
		}) error {
			resolvedTs := atomic.LoadUint64(&sink.resolvedTs)
			if resolvedTs < globalResolvedTs {
				globalResolvedTs = resolvedTs
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}

		todoDDL = c.getFrontDDL()
		if todoDDL != nil && todoDDL.Ts < globalResolvedTs {
			globalResolvedTs = todoDDL.Ts
		}
		if lastGlobalResolvedTs == globalResolvedTs {
			continue
		}
		lastGlobalResolvedTs = globalResolvedTs
		atomic.StoreUint64(&c.globalResolvedTs, globalResolvedTs)
		log.Debug("update globalResolvedTs", zap.Uint64("ts", globalResolvedTs))

		err = c.forEachSink(func(sink *struct {
			sink.Sink
			resolvedTs uint64
		}) error {
			return sink.EmitResolvedEvent(ctx, globalResolvedTs)
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
}

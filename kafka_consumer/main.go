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

	"github.com/pingcap/ticdc/pkg/config"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	cdcfilter "github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
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
	timezone string
)

func init() {
	var upstreamURIStr string

	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "Kafka uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&logPath, "log-file", "cdc_kafka_consumer.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log file path")
	flag.StringVar(&timezone, "tz", "System", "Specify time zone of Kafka consumer")
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
	tz := time.Local
	if strings.ToLower(timezone) != "system" {
		var err error
		tz, err = time.LoadLocation(timezone)
		if err != nil {
			return nil, errors.Annotate(err, "can not load timezone")
		}
	}
	ctx = util.PutTimezoneInCtx(ctx, tz)
	filter, err := cdcfilter.NewFilter(config.GetDefaultReplicaConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := new(Consumer)
	c.sinks = make([]*struct {
		sink.Sink
		resolvedTs uint64
	}, kafkaPartitionNum)
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	for i := 0; i < int(kafkaPartitionNum); i++ {
		s, err := sink.NewSink(ctx, "kafka-consumer", downstreamURIStr, filter, config.GetDefaultReplicaConfig(), nil, errCh)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}
		c.sinks[i] = &struct {
			sink.Sink
			resolvedTs uint64
		}{Sink: s}
	}
	sink, err := sink.NewSink(ctx, "kafka-consumer", downstreamURIStr, filter, config.GetDefaultReplicaConfig(), nil, errCh)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	go func() {
		err := <-errCh
		if errors.Cause(err) != context.Canceled {
			log.Error("error on running consumer", zap.Error(err))
		} else {
			log.Info("consumer exited")
		}
		cancel()
	}()
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
ClaimMessages:
	for message := range claim.Messages() {
		log.Info("Message claimed", zap.Int32("partition", message.Partition), zap.ByteString("key", message.Key), zap.ByteString("value", message.Value))
		batchDecoder, err := codec.NewJSONEventBatchDecoder(message.Key, message.Value)
		if err != nil {
			return errors.Trace(err)
		}
		for {
			tp, hasNext, err := batchDecoder.HasNext()
			if err != nil {
				log.Fatal("decode message key failed", zap.Error(err))
			}
			if !hasNext {
				break
			}
			switch tp {
			case model.MqMessageTypeDDL:
				ddl, err := batchDecoder.NextDDLEvent()
				if err != nil {
					log.Fatal("decode message value failed", zap.ByteString("value", message.Value))
				}
				c.appendDDL(ddl)
			case model.MqMessageTypeRow:
				row, err := batchDecoder.NextRowChangedEvent()
				if err != nil {
					log.Fatal("decode message value failed", zap.ByteString("value", message.Value))
				}
				globalResolvedTs := atomic.LoadUint64(&c.globalResolvedTs)
				if row.CommitTs <= globalResolvedTs || row.CommitTs <= sink.resolvedTs {
					log.Debug("filter fallback row", zap.ByteString("row", message.Key),
						zap.Uint64("globalResolvedTs", globalResolvedTs),
						zap.Uint64("sinkResolvedTs", sink.resolvedTs),
						zap.Int32("partition", partition))
					break ClaimMessages
				}
				// FIXME: hack to set start-ts in row changed event, as start-ts
				// is not contained in TiCDC open protocol
				row.StartTs = row.CommitTs
				err = sink.EmitRowChangedEvents(ctx, row)
				if err != nil {
					log.Fatal("emit row changed event failed", zap.Error(err))
				}
			case model.MqMessageTypeResolved:
				ts, err := batchDecoder.NextResolvedEvent()
				if err != nil {
					log.Fatal("decode message value failed", zap.ByteString("value", message.Value))
				}
				resolvedTs := atomic.LoadUint64(&sink.resolvedTs)
				if resolvedTs < ts {
					log.Debug("update sink resolved ts",
						zap.Uint64("ts", ts),
						zap.Int32("partition", partition))
					atomic.StoreUint64(&sink.resolvedTs, ts)
				}
			}
			session.MarkMessage(message, "")
		}
	}

	return nil
}

func (c *Consumer) appendDDL(ddl *model.DDLEvent) {
	c.ddlListMu.Lock()
	defer c.ddlListMu.Unlock()
	if ddl.CommitTs <= c.maxDDLReceivedTs {
		return
	}
	globalResolvedTs := atomic.LoadUint64(&c.globalResolvedTs)
	if ddl.CommitTs <= globalResolvedTs {
		log.Error("unexpected ddl job", zap.Uint64("ddlts", ddl.CommitTs), zap.Uint64("globalResolvedTs", globalResolvedTs))
		return
	}
	c.ddlList = append(c.ddlList, ddl)
	c.maxDDLReceivedTs = ddl.CommitTs
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
	var lastGlobalResolvedTs uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		time.Sleep(100 * time.Millisecond)
		// handle ddl
		globalResolvedTs := uint64(math.MaxUint64)
		err := c.forEachSink(func(sink *struct {
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
		todoDDL := c.getFrontDDL()
		if todoDDL != nil && globalResolvedTs >= todoDDL.CommitTs {
			//flush DMLs
			err := c.forEachSink(func(sink *struct {
				sink.Sink
				resolvedTs uint64
			}) error {
				return sink.FlushRowChangedEvents(ctx, todoDDL.CommitTs)
			})
			if err != nil {
				return errors.Trace(err)
			}

			// execute ddl
			err = c.ddlSink.EmitDDLEvent(ctx, todoDDL)
			if err != nil {
				return errors.Trace(err)
			}
			c.popDDL()
			continue
		}

		if todoDDL != nil && todoDDL.CommitTs < globalResolvedTs {
			globalResolvedTs = todoDDL.CommitTs
		}
		if lastGlobalResolvedTs == globalResolvedTs {
			continue
		}
		lastGlobalResolvedTs = globalResolvedTs
		atomic.StoreUint64(&c.globalResolvedTs, globalResolvedTs)
		log.Info("update globalResolvedTs", zap.Uint64("ts", globalResolvedTs))

		err = c.forEachSink(func(sink *struct {
			sink.Sink
			resolvedTs uint64
		}) error {
			return sink.FlushRowChangedEvents(ctx, globalResolvedTs)
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
}

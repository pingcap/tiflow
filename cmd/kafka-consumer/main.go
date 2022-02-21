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

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/config"
	cdcfilter "github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Sarama configuration options
var (
	kafkaAddrs           []string
	kafkaTopic           string
	kafkaPartitionNum    int32
	kafkaGroupID         = fmt.Sprintf("ticdc_kafka_consumer_%s", uuid.New().String())
	kafkaVersion         = "2.4.0"
	kafkaMaxMessageBytes = math.MaxInt64
	kafkaMaxBatchSize    = math.MaxInt64

	downstreamURIStr string

	logPath       string
	logLevel      string
	timezone      string
	ca, cert, key string
)

func init() {
	var upstreamURIStr string

	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "Kafka uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&logPath, "log-file", "cdc_kafka_consumer.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log file path")
	flag.StringVar(&timezone, "tz", "System", "Specify time zone of Kafka consumer")
	flag.StringVar(&ca, "ca", "", "CA certificate path for Kafka SSL connection")
	flag.StringVar(&cert, "cert", "", "Certificate path for Kafka SSL connection")
	flag.StringVar(&key, "key", "", "Private key path for Kafka SSL connection")
	flag.Parse()

	err := logutil.InitLogger(&logutil.Config{
		Level: logLevel,
		File:  logPath,
	})
	if err != nil {
		log.Panic("init logger failed", zap.Error(err))
	}

	upstreamURI, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Panic("invalid upstream-uri", zap.Error(err))
	}
	scheme := strings.ToLower(upstreamURI.Scheme)
	if scheme != "kafka" {
		log.Panic("invalid upstream-uri scheme, the scheme of upstream-uri must be `kafka`",
			zap.String("upstreamURI", upstreamURIStr))
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
		log.Panic("Error creating sarama config", zap.Error(err))
	}

	s = upstreamURI.Query().Get("partition-num")
	if s == "" {
		partition, err := getPartitionNum(kafkaAddrs, kafkaTopic, config)
		if err != nil {
			log.Panic("can not get partition number", zap.String("topic", kafkaTopic), zap.Error(err))
		}
		kafkaPartitionNum = partition
	} else {
		c, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			log.Panic("invalid partition-num of upstream-uri")
		}
		kafkaPartitionNum = int32(c)
	}

	s = upstreamURI.Query().Get("max-message-bytes")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			log.Panic("invalid max-message-bytes of upstream-uri")
		}
		log.Info("Setting max-message-bytes", zap.Int("max-message-bytes", c))
		kafkaMaxMessageBytes = c
	}

	s = upstreamURI.Query().Get("max-batch-size")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			log.Panic("invalid max-batch-size of upstream-uri")
		}
		log.Info("Setting max-batch-size", zap.Int("max-batch-size", c))
		kafkaMaxBatchSize = c
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

	if len(ca) != 0 {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config, err = (&security.Credential{
			CAPath:   ca,
			CertPath: cert,
			KeyPath:  key,
		}).ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

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
		log.Panic("Error creating sarama config", zap.Error(err))
	}
	err = waitTopicCreated(kafkaAddrs, kafkaTopic, config)
	if err != nil {
		log.Panic("wait topic created failed", zap.Error(err))
	}
	/**
	 * Setup a new Sarama consumer group
	 */
	consumer, err := NewConsumer(context.TODO())
	if err != nil {
		log.Panic("Error creating consumer", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(kafkaAddrs, kafkaGroupID, config)
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
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
				log.Panic("Error from consumer: %v", zap.Error(err))
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
			log.Panic("Error running consumer: %v", zap.Error(err))
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
		log.Panic("Error closing client", zap.Error(err))
	}
}

type partitionSink struct {
	sink.Sink
	resolvedTs  uint64
	partitionNo int
	tablesMap   sync.Map
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool

	ddlList          []*model.DDLEvent
	maxDDLReceivedTs uint64
	ddlListMu        sync.Mutex

	sinks   []*partitionSink
	sinksMu sync.Mutex

	ddlSink              sink.Sink
	fakeTableIDGenerator *fakeTableIDGenerator

	globalResolvedTs uint64
}

// NewConsumer creates a new cdc kafka consumer
func NewConsumer(ctx context.Context) (*Consumer, error) {
	// TODO support filter in downstream sink
	tz, err := util.GetTimezone(timezone)
	if err != nil {
		return nil, errors.Annotate(err, "can not load timezone")
	}
	ctx = util.PutTimezoneInCtx(ctx, tz)
	filter, err := cdcfilter.NewFilter(config.GetDefaultReplicaConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := new(Consumer)
	c.fakeTableIDGenerator = &fakeTableIDGenerator{
		tableIDs: make(map[string]int64),
	}
	c.sinks = make([]*partitionSink, kafkaPartitionNum)
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	opts := map[string]string{}
	for i := 0; i < int(kafkaPartitionNum); i++ {
		s, err := sink.New(ctx, "kafka-consumer", downstreamURIStr, filter, config.GetDefaultReplicaConfig(), opts, errCh)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}
		c.sinks[i] = &partitionSink{Sink: s, partitionNo: i}
	}
	sink, err := sink.New(ctx, "kafka-consumer", downstreamURIStr, filter, config.GetDefaultReplicaConfig(), opts, errCh)
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

	for message := range claim.Messages() {
		log.Debug("Message claimed", zap.Int32("partition", message.Partition), zap.ByteString("key", message.Key), zap.ByteString("value", message.Value))
		batchDecoder, err := codec.NewJSONEventBatchDecoder(message.Key, message.Value)
		if err != nil {
			return errors.Trace(err)
		}

		counter := 0
		for {
			tp, hasNext, err := batchDecoder.HasNext()
			if err != nil {
				log.Panic("decode message key failed", zap.Error(err))
			}
			if !hasNext {
				break
			}

			counter++
			// If the message containing only one event exceeds the length limit, CDC will allow it and issue a warning.
			if len(message.Key)+len(message.Value) > kafkaMaxMessageBytes && counter > 1 {
				log.Panic("kafka max-messages-bytes exceeded", zap.Int("max-message-bytes", kafkaMaxMessageBytes),
					zap.Int("receviedBytes", len(message.Key)+len(message.Value)))
			}

			switch tp {
			case model.MqMessageTypeDDL:
				ddl, err := batchDecoder.NextDDLEvent()
				if err != nil {
					log.Panic("decode message value failed", zap.ByteString("value", message.Value))
				}
				c.appendDDL(ddl)
			case model.MqMessageTypeRow:
				row, err := batchDecoder.NextRowChangedEvent()
				if err != nil {
					log.Panic("decode message value failed", zap.ByteString("value", message.Value))
				}
				globalResolvedTs := atomic.LoadUint64(&c.globalResolvedTs)
				if row.CommitTs <= globalResolvedTs || row.CommitTs <= sink.resolvedTs {
					log.Debug("RowChangedEvent fallback row, ignore it",
						zap.Uint64("commitTs", row.CommitTs),
						zap.Uint64("globalResolvedTs", globalResolvedTs),
						zap.Uint64("sinkResolvedTs", sink.resolvedTs),
						zap.Int32("partition", partition),
						zap.ByteString("row", message.Key))
				}
				// FIXME: hack to set start-ts in row changed event, as start-ts
				// is not contained in TiCDC open protocol
				row.StartTs = row.CommitTs
				var partitionID int64
				if row.Table.IsPartition {
					partitionID = row.Table.TableID
				}
				row.Table.TableID =
					c.fakeTableIDGenerator.generateFakeTableID(row.Table.Schema, row.Table.Table, partitionID)
				err = sink.EmitRowChangedEvents(ctx, row)
				if err != nil {
					log.Panic("emit row changed event failed", zap.Error(err))
				}
				lastCommitTs, ok := sink.tablesMap.Load(row.Table.TableID)
				if !ok || lastCommitTs.(uint64) < row.CommitTs {
					sink.tablesMap.Store(row.Table.TableID, row.CommitTs)
				}
			case model.MqMessageTypeResolved:
				ts, err := batchDecoder.NextResolvedEvent()
				if err != nil {
					log.Panic("decode message value failed", zap.ByteString("value", message.Value))
				}
				resolvedTs := atomic.LoadUint64(&sink.resolvedTs)
				// `resolvedTs` should be monotonically increasing, it's allowed to receive redandunt one.
				if ts < resolvedTs {
					log.Panic("partition resolved ts fallback",
						zap.Uint64("ts", ts),
						zap.Uint64("resolvedTs", resolvedTs),
						zap.Int32("partition", partition))
				} else if ts > resolvedTs {
					log.Debug("update sink resolved ts",
						zap.Uint64("ts", ts),
						zap.Int32("partition", partition))
					atomic.StoreUint64(&sink.resolvedTs, ts)
				} else {
					log.Info("redundant sink resolved ts", zap.Uint64("ts", ts), zap.Int32("partition", partition))
				}
			}
			session.MarkMessage(message, "")
		}

		if counter > kafkaMaxBatchSize {
			log.Panic("Open Protocol max-batch-size exceeded", zap.Int("max-batch-size", kafkaMaxBatchSize),
				zap.Int("actual-batch-size", counter))
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
	if ddl.CommitTs < globalResolvedTs {
		log.Panic("unexpected ddl job", zap.Uint64("ddlts", ddl.CommitTs), zap.Uint64("globalResolvedTs", globalResolvedTs))
	}
	if ddl.CommitTs == globalResolvedTs {
		log.Warn("receive redundant ddl job", zap.Uint64("ddlts", ddl.CommitTs), zap.Uint64("globalResolvedTs", globalResolvedTs))
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

func (c *Consumer) forEachSink(fn func(sink *partitionSink) error) error {
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
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		// initialize the `globalResolvedTs` as min of all partition's `ResolvedTs`
		globalResolvedTs := uint64(math.MaxUint64)
		err := c.forEachSink(func(sink *partitionSink) error {
			resolvedTs := atomic.LoadUint64(&sink.resolvedTs)
			if resolvedTs < globalResolvedTs {
				globalResolvedTs = resolvedTs
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		// handle ddl
		todoDDL := c.getFrontDDL()
		if todoDDL != nil && globalResolvedTs >= todoDDL.CommitTs {
			// flush DMLs
			err := c.forEachSink(func(sink *partitionSink) error {
				return syncFlushRowChangedEvents(ctx, sink, todoDDL.CommitTs)
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
		if lastGlobalResolvedTs > globalResolvedTs {
			log.Panic("global ResolvedTs fallback")
		}

		if globalResolvedTs > lastGlobalResolvedTs {
			lastGlobalResolvedTs = globalResolvedTs
			atomic.StoreUint64(&c.globalResolvedTs, globalResolvedTs)
			log.Info("update globalResolvedTs", zap.Uint64("ts", globalResolvedTs))

			err = c.forEachSink(func(sink *partitionSink) error {
				return syncFlushRowChangedEvents(ctx, sink, globalResolvedTs)
			})
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func syncFlushRowChangedEvents(ctx context.Context, sink *partitionSink, resolvedTs uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// tables are flushed
		var (
			err          error
			checkpointTs uint64
		)
		flushedResolvedTs := true
		sink.tablesMap.Range(func(key, value interface{}) bool {
			tableID := key.(int64)
			checkpointTs, err = sink.FlushRowChangedEvents(ctx, tableID, resolvedTs)
			if err != nil {
				return false
			}
			if checkpointTs < resolvedTs {
				flushedResolvedTs = false
			}
			return true
		})
		if err != nil {
			return err
		}
		if flushedResolvedTs {
			return nil
		}
	}
}

type fakeTableIDGenerator struct {
	tableIDs       map[string]int64
	currentTableID int64
	mu             sync.Mutex
}

func (g *fakeTableIDGenerator) generateFakeTableID(schema, table string, partition int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := quotes.QuoteSchema(schema, table)
	if partition != 0 {
		key = fmt.Sprintf("%s.`%d`", key, partition)
	}
	if tableID, ok := g.tableIDs[key]; ok {
		return tableID
	}
	g.currentTableID++
	g.tableIDs[key] = g.currentTableID
	return g.currentTableID
}

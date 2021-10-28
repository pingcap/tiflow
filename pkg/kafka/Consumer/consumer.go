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

package Consumer

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/quotes"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	Ready chan bool

	ddlList          []*model.DDLEvent
	maxDDLReceivedTs uint64
	ddlListMu        sync.Mutex

	sinks []*struct {
		sink.Sink
		resolvedTs uint64
	}
	sinksMu sync.Mutex

	ddlSink              sink.Sink
	fakeTableIDGenerator *fakeTableIDGenerator

	globalResolvedTs uint64
	maxMessageBytes  int
	maxBatchSize     int
}

// NewConsumer creates a new cdc kafka consumer
func (c *Config) NewConsumer(ctx context.Context) (*Consumer, error) {
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

	consumer := &Consumer{
		maxMessageBytes: c.maxMessageBytes,
		maxBatchSize:    c.maxBatchSize,

		fakeTableIDGenerator: &fakeTableIDGenerator{
			tableIDs: make(map[string]int64),
		},
		sinks: make([]*struct {
			sink.Sink
			resolvedTs uint64
		}, c.partitionCount),
	}

	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	opts := map[string]string{}
	for i := 0; i < int(c.partitionCount); i++ {
		s, err := sink.New(ctx, c.changefeedID, c.downstreamStr, filter, config.GetDefaultReplicaConfig(), opts, errCh)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}
		consumer.sinks[i] = &struct {
			sink.Sink
			resolvedTs uint64
		}{Sink: s}
	}
	sink, err := sink.New(ctx, c.changefeedID, c.downstreamStr, filter, config.GetDefaultReplicaConfig(), opts, errCh)
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
	consumer.ddlSink = sink
	consumer.Ready = make(chan bool)
	return consumer, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the c as ready
	close(c.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) GetDecoder() codec.EventBatchDecoder {
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

		counter := 0
		for {
			tp, hasNext, err := batchDecoder.HasNext()
			if err != nil {
				log.Fatal("decode message key failed", zap.Error(err))
			}
			if !hasNext {
				break
			}

			counter++
			// If the message containing only one event exceeds the length limit, CDC will allow it and issue a warning.
			if len(message.Key)+len(message.Value) > c.maxMessageBytes && counter > 1 {
				log.Fatal("kafka max-messages-bytes exceeded", zap.Int("max-message-bytes", c.maxMessageBytes),
					zap.Int("received-bytes", len(message.Key)+len(message.Value)))
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
				var partitionID int64
				if row.Table.IsPartition {
					partitionID = row.Table.TableID
				}
				row.Table.TableID =
					c.fakeTableIDGenerator.generateFakeTableID(row.Table.Schema, row.Table.Table, partitionID)
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

		if counter > c.maxBatchSize {
			log.Fatal("Open Protocol max-batch-size exceeded", zap.Int("max-batch-size", c.maxBatchSize),
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
			// flush DMLs
			err := c.forEachSink(func(sink *struct {
				sink.Sink
				resolvedTs uint64
			}) error {
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
			return syncFlushRowChangedEvents(ctx, sink, globalResolvedTs)
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func syncFlushRowChangedEvents(ctx context.Context, sink sink.Sink, resolvedTs uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		checkpointTs, err := sink.FlushRowChangedEvents(ctx, resolvedTs)
		if err != nil {
			return err
		}
		if checkpointTs >= resolvedTs {
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

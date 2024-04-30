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
	"strings"
	"time"

	"github.com/IBM/sarama"
	cerror "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

func newConfig(o *consumerOption) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(o.version)
	if err != nil {
		return nil, cerror.Trace(err)
	}

	config.ClientID = "ticdc_kafka_sarama_consumer"
	config.Version = version

	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	if len(o.ca) != 0 {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config, err = (&security.Credential{
			CAPath:   o.ca,
			CertPath: o.cert,
			KeyPath:  o.key,
		}).ToTLSConfig()
		if err != nil {
			return nil, cerror.Trace(err)
		}
	}

	return config, err
}

func saramGetPartitionNum(address []string, topic string, cfg *sarama.Config) (int32, error) {
	// get partition number or create topic automatically
	admin, err := sarama.NewClusterAdmin(address, cfg)
	if err != nil {
		return 0, cerror.Trace(err)
	}
	defer admin.Close()
	for i := 0; i <= 30; i++ {
		topics, err := admin.ListTopics()
		if err != nil {
			return 0, cerror.Trace(err)
		}
		if topicDetail, ok := topics[topic]; ok {
			log.Info("get partition number of topic",
				zap.String("topic", topic),
				zap.Int32("partitionNum", topicDetail.NumPartitions))
			return topicDetail.NumPartitions, nil
		}
		log.Info("retry get partition number", zap.String("topic", topic))
		time.Sleep(1 * time.Second)
	}

	return 0, cerror.Errorf("get partition number(%s) timeout", topic)
}

type saramConsumer struct {
	ready  chan bool
	option *consumerOption
	config *sarama.Config
	writer *writer
}

var _ KakfaConsumer = (*saramConsumer)(nil)

// NewSaramConsumer will create a consumer client.
func NewSaramConsumer(ctx context.Context, o *consumerOption) KakfaConsumer {
	c := new(saramConsumer)
	w, err := NewWriter(ctx, o)
	if err != nil {
		log.Panic("Error creating writer", zap.Error(err))
	}
	config, err := newConfig(o)
	if err != nil {
		log.Panic("Error creating sarama config", zap.Error(err))
	}
	partitionNum, err := saramGetPartitionNum(o.address, o.topic, config)
	if err != nil {
		log.Panic("Error get partition number", zap.String("topic", o.topic), zap.Error(err))
	}
	if o.partitionNum == 0 {
		o.partitionNum = partitionNum
	}
	c.writer = w
	c.option = o
	c.ready = make(chan bool)
	c.config = config
	// async write to downstream
	go c.AsyncWrite(ctx)
	return c
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *saramConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the c as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *saramConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim will async read message from Kafka
func (c *saramConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	partition := claim.Partition()
	log.Info("start consume claim",
		zap.String("topic", claim.Topic()), zap.Int32("partition", partition),
		zap.Int64("initialOffset", claim.InitialOffset()), zap.Int64("highWaterMarkOffset", claim.HighWaterMarkOffset()))
	// create new decoder
	decoder, err := NewDecoder(context.Background(), c.option, c.writer.upstreamTiDB)
	if err != nil {
		log.Panic("Error create decoder", zap.Error(err))
	}
	eventGroups := make(map[int64]*eventsGroup)
	for message := range claim.Messages() {
		if err := c.writer.Decode(decoder, c.option, partition, message.Key, message.Value, eventGroups); err != nil {
			return err
		}
		// sync write to downstream
		// if err := c.writer.Write(context.Background()); err != nil {
		// 	log.Panic("Error write to downstream", zap.Error(err))
		// }
		// session.MarkMessage(message, "")
	}
	return nil
}

// Consume will read message from Kafka.
func (c *saramConsumer) Consume(ctx context.Context) error {
	client, err := sarama.NewConsumerGroup(c.option.address, c.option.groupID, c.config)
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
	}

	defer func() {
		if err = client.Close(); err != nil {
			log.Panic("Error closing client", zap.Error(err))
		}
	}()

	for {
		// `consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := client.Consume(ctx, strings.Split(c.option.topic, ","), c); err != nil {
			log.Panic("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.ready = make(chan bool)
	}
}

// AsyncWrite call writer to write to the downsteam asynchronously.
func (c *saramConsumer) AsyncWrite(ctx context.Context) {
	if err := c.writer.AsyncWrite(ctx); err != nil {
		log.Info("async write break", zap.Error(err))
	}
}

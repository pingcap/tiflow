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
	"sync"
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
	config.Consumer.Offsets.AutoCommit.Enable = false

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

func saramaGetPartitionNum(o *consumerOption, cfg *sarama.Config) (int32, error) {
	admin, err := sarama.NewClusterAdmin(o.address, cfg)
	if err != nil {
		return 0, cerror.Trace(err)
	}
	defer admin.Close()
	for i := 0; i <= o.retryTime; i++ {
		topics, err := admin.ListTopics()
		if err != nil {
			return 0, cerror.Trace(err)
		}
		if topicDetail, ok := topics[o.topic]; ok {
			log.Info("get partition number of topic",
				zap.String("topic", o.topic),
				zap.Int32("partitionNum", topicDetail.NumPartitions))
			return topicDetail.NumPartitions, nil
		}
		log.Info("retry get partition number", zap.String("topic", o.topic))
		time.Sleep(1 * time.Second)
	}

	return 0, cerror.Errorf("get partition number(%s) timeout", o.topic)
}

type saramaConsumer struct {
	mu     sync.Mutex
	option *consumerOption
	config *sarama.Config
	writer *writer
}

var _ KakfaConsumer = (*saramaConsumer)(nil)

// NewSaramaConsumer will create a consumer client.
func NewSaramaConsumer(ctx context.Context, o *consumerOption) KakfaConsumer {
	c := new(saramaConsumer)
	config, err := newConfig(o)
	if err != nil {
		log.Panic("Error creating sarama config", zap.Error(err))
	}
	partitionNum, err := saramaGetPartitionNum(o, config)
	if err != nil {
		log.Panic("Error get partition number", zap.String("topic", o.topic), zap.Error(err))
	}
	if o.partitionNum == 0 {
		o.partitionNum = partitionNum
	}
	w, err := NewWriter(ctx, o)
	if err != nil {
		log.Panic("Error creating writer", zap.Error(err))
	}
	c.writer = w
	c.option = o
	c.config = config
	return c
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *saramaConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *saramaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim will async read message from Kafka
func (c *saramaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	partition := claim.Partition()
	log.Info("start consume claim",
		zap.String("topic", claim.Topic()), zap.Int32("partition", partition),
		zap.Int64("initialOffset", claim.InitialOffset()), zap.Int64("highWaterMarkOffset", claim.HighWaterMarkOffset()))
	ctx := context.Background()
	for msg := range claim.Messages() {
		// sync decode and write
		c.mu.Lock()
		needCommit, err := c.writer.Decode(ctx, c.option, partition, msg.Key, msg.Value)
		if err != nil {
			log.Error("Error decode message", zap.Error(err))
		}
		if needCommit {
			session.MarkMessage(msg, "")
			session.Commit()
		}
		c.mu.Unlock()
	}
	return nil
}

// Consume will read message from Kafka.
func (c *saramaConsumer) Consume(ctx context.Context) error {
	client, err := sarama.NewConsumerGroup(c.option.address, c.option.groupID, c.config)
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
	}
	topics := strings.Split(c.option.topic, ",")
	if len(topics) == 0 {
		log.Panic("Error no topics provided")
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
		if err := client.Consume(ctx, topics, c); err != nil {
			log.Error("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

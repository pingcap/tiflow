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
	"fmt"
	"strings"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	cerror "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func confluentGetPartitionNum(o *consumerOption) (int32, error) {
	configMap := &confluent.ConfigMap{
		"bootstrap.servers": strings.Join(o.address, ","),
	}
	if len(o.ca) != 0 {
		configMap.SetKey("security.protocol", "SSL")
		configMap.SetKey("ssl.ca.location", o.ca)
		configMap.SetKey("ssl.key.location", o.key)
		configMap.SetKey("ssl.ca.location", o.cert)
	}
	admin, err := confluent.NewAdminClient(configMap)
	if err != nil {
		return 0, cerror.Trace(err)
	}
	defer admin.Close()

	for i := 0; i <= o.retryTime; i++ {
		resp, err := admin.GetMetadata(&o.topic, false, 500)
		if err != nil {
			return 0, cerror.Trace(err)
		}
		if topicDetail, ok := resp.Topics[o.topic]; ok {
			numPartitions := int32(len(topicDetail.Partitions))
			log.Info("get partition number of topic",
				zap.String("topic", o.topic),
				zap.Int32("partitionNum", numPartitions))
			return numPartitions, nil
		}
		log.Info("retry get partition number", zap.String("topic", o.topic))
		time.Sleep(1 * time.Second)
	}
	return 0, cerror.Errorf("get partition number(%s) timeout", o.topic)
}

type ConfluentConsumer struct {
	option *consumerOption
	writer *writer
}

var _ KakfaConsumer = (*ConfluentConsumer)(nil)

func NewConfluentConsumer(ctx context.Context, o *consumerOption) KakfaConsumer {
	c := new(ConfluentConsumer)
	partitionNum, err := confluentGetPartitionNum(o)
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
	// async write to downstream
	// go c.AsyncWrite(ctx)
	return c
}

func (c *ConfluentConsumer) Consume(ctx context.Context) error {
	topics := strings.Split(c.option.topic, ",")
	if len(topics) == 0 {
		log.Panic("Error no topics provided")
	}
	configMap := &confluent.ConfigMap{
		"bootstrap.servers":  strings.Join(c.option.address, ","),
		"group.id":           c.option.groupID,
		"session.timeout.ms": 6000,
		// Start reading from the first message of each assigned
		// partition if there are no previously committed offsets
		// for this group.
		"auto.offset.reset": "earliest",
		// Whether or not we store offsets automatically.
		"enable.auto.offset.store": false,
	}
	if len(c.option.ca) != 0 {
		configMap.SetKey("security.protocol", "SSL")
		configMap.SetKey("ssl.ca.location", c.option.ca)
		configMap.SetKey("ssl.key.location", c.option.key)
		configMap.SetKey("ssl.ca.location", c.option.cert)
	}
	client, err := confluent.NewConsumer(configMap)
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
	}
	defer func() {
		if err = client.Close(); err != nil {
			log.Panic("Error closing client", zap.Error(err))
		}
	}()

	err = client.SubscribeTopics(topics, nil)

	decoder, err := NewDecoder(ctx, c.option, c.writer.upstreamTiDB)
	if err != nil {
		log.Panic("Error create decoder", zap.Error(err))
	}
	eventGroups := make(map[int64]*eventsGroup)
	for {
		msg, err := client.ReadMessage(100 * time.Millisecond)
		if err == nil {
			// Process the message received.
			partition := msg.TopicPartition.Partition
			if err := c.writer.Decode(decoder, c.option, partition, msg.Key, msg.Value, eventGroups); err != nil {
				log.Panic("Error decode message", zap.Error(err))
			}
			// sync write to downstream
			if err := c.writer.Write(ctx); err != nil {
				log.Panic("Error write to downstream", zap.Error(err))
			}
			client.StoreMessage(msg)
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(confluent.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

// async write to downsteam
func (c *ConfluentConsumer) AsyncWrite(ctx context.Context) {
	if err := c.writer.AsyncWrite(ctx); err != nil {
		log.Info("async write break", zap.Error(err))
	}
}

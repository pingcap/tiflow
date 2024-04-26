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

// import (
// 	"context"
// 	"strings"
// 	"time"

// 	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
// 	cerror "github.com/pingcap/errors"
// 	"github.com/pingcap/log"
// 	"go.uber.org/zap"
// )

// func confluentGetPartitionNum(address []string, topic string) (int32, error) {
// 	admin, err := confluent.NewAdminClient(&confluent.ConfigMap{
// 		"bootstrap.servers": strings.Join(address, ","),
// 	})
// 	if err != nil {
// 		return 0, cerror.Trace(err)
// 	}
// 	defer admin.Close()

// 	for i := 0; i <= 30; i++ {
// 		resp, err := admin.GetMetadata(&topic, false, 500)
// 		if err != nil {
// 			return 0, cerror.Trace(err)
// 		}
// 		if topicDetail, ok := resp.Topics[topic]; ok {
// 			numPartitions := int32(len(topicDetail.Partitions))
// 			log.Info("get partition number of topic",
// 				zap.String("topic", topic),
// 				zap.Int32("partitionNum", numPartitions))
// 			return numPartitions, nil
// 		}
// 		log.Info("retry get partition number", zap.String("topic", topic))
// 		time.Sleep(1 * time.Second)
// 	}
// 	return 0, cerror.Errorf("get partition number(%s) timeout", topic)
// }

// type ConfluentConsumer struct {
// 	option *ConsumerOption
// 	writer *Writer
// }

// var _ KakfaConsumer = (*ConfluentConsumer)(nil)

// func NewConfluentConsumer(ctx context.Context, o *ConsumerOption) KakfaConsumer {
// 	c := new(ConfluentConsumer)
// 	w, err := NewWriter(ctx, o)
// 	if err != nil {
// 		log.Panic("Error creating writer", zap.Error(err))
// 	}
// 	partitionNum, err := confluentGetPartitionNum(o.address, o.topic)
// 	if err != nil {
// 		log.Panic("Error get partition number", zap.String("topic", o.topic), zap.Error(err))
// 	}
// 	if o.partitionNum == 0 {
// 		o.partitionNum = partitionNum
// 	}
// 	c.writer = w
// 	c.option = o
// 	// async write to downstream
// 	go c.AsyncWrite(ctx)
// 	return c
// }

// func (c *ConfluentConsumer) Consume(ctx context.Context) error {
// 	topics := strings.Split(c.option.topic, ",")
// 	if len(topics) == 0 {
// 		log.Panic("Error no topics provided")
// 	}
// 	client, err := confluent.NewConsumer(&confluent.ConfigMap{
// 		"bootstrap.servers":  strings.Join(c.option.address, ","),
// 		"group.id":           c.option.groupID,
// 		"session.timeout.ms": 6000,
// 		// Start reading from the first message of each assigned
// 		// partition if there are no previously committed offsets
// 		// for this group.
// 		"auto.offset.reset": "earliest",
// 		// Whether or not we store offsets automatically.
// 		"enable.auto.offset.store": false,
// 	})
// 	if err != nil {
// 		log.Panic("Error creating consumer group client", zap.Error(err))
// 	}
// 	defer func() {
// 		if err = client.Close(); err != nil {
// 			log.Panic("Error closing client", zap.Error(err))
// 		}
// 	}()

// 	err = client.SubscribeTopics(topics, nil)

// 	for {
// 		ev := client.Poll(100)
// 		if ev == nil {
// 			continue
// 		}
// 		eventGroups := make(map[int64]*EventsGroup)
// 		switch e := ev.(type) {
// 		case *confluent.Message:
// 			// Process the message received.
// 			partition := e.TopicPartition.Partition
// 			if err := c.writer.Decode(ctx, c.option, partition, e.Key, e.Value, eventGroups); err != nil {
// 				log.Panic("Error decode message", zap.Error(err))
// 			}
// 			// sync write to downstream
// 			// if err := c.writer.Write(ctx); err != nil {
// 			// 	log.Panic("Error write to downstream", zap.Error(err))
// 			// }
// 			// client.StoreMessage(e)
// 		case confluent.Error:
// 			// Errors should generally be considered
// 			// informational, the client will try to
// 			// automatically recover.
// 			if e.Code() == confluent.ErrAllBrokersDown {
// 				log.Panic("Error all brokers down", zap.Error(e))
// 			}
// 		default:
// 		}

// 	}
// }

// // async write to downsteam
// func (c *ConfluentConsumer) AsyncWrite(ctx context.Context) {
// 	c.writer.AsyncWrite(ctx)
// }

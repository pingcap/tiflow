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
	"errors"
	"strings"
	"time"

	cerror "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func kafkaGoGetPartitionNum(address []string, topic string) (int32, error) {
	client := &kafka.Client{
		Addr:    kafka.TCP(address...),
		Timeout: 10 * time.Second,
	}
	for i := 0; i <= 30; i++ {
		resp, err := client.Metadata(context.Background(), &kafka.MetadataRequest{})
		if err != nil {
			return 0, cerror.Trace(err)
		}
		topics := resp.Topics
		for i := 0; i < len(topics); i++ {
			if topics[i].Name == topic {
				topicDetail := topics[i]
				numPartitions := int32(len(topicDetail.Partitions))
				log.Info("get partition number of topic",
					zap.String("topic", topic),
					zap.Int32("partitionNum", numPartitions))
				return numPartitions, nil
			}
		}
		log.Info("retry get partition number", zap.String("topic", topic))
		time.Sleep(1 * time.Second)
	}
	return 0, cerror.Errorf("wait the topic(%s) created timeout", topic)
}

func kafkaGoWaitTopicCreated(address []string, topic string) error {
	client := &kafka.Client{
		Addr: kafka.TCP(address...),
		// todo: make this configurable
		Timeout: 10 * time.Second,
		// Transport: cfg. transport,
	}
	for i := 0; i <= 30; i++ {
		resp, err := client.Metadata(context.Background(), &kafka.MetadataRequest{})
		if err != nil {
			return cerror.Trace(err)
		}
		topics := resp.Topics
		for i := 0; i < len(topics); i++ {
			if topics[i].Name == topic {
				return nil
			}
		}
		log.Info("wait the topic created", zap.String("topic", topic))
		time.Sleep(1 * time.Second)
	}
	return cerror.Errorf("wait the topic(%s) created timeout", topic)

}

type KafkaGoConsumer struct {
	ready  chan bool
	option *ConsumerOption
	writer *Writer
}

var _ KakfaConsumer = (*KafkaGoConsumer)(nil)

func NewkafkaGoConsumer(ctx context.Context, o *ConsumerOption) KakfaConsumer {
	c := new(KafkaGoConsumer)
	w, err := NewWriter(ctx, o)
	if err != nil {
		log.Panic("Error creating writer", zap.Error(err))
	}
	partitionNum, err := kafkaGoGetPartitionNum(o.address, o.topic)
	if err != nil {
		log.Panic("Error get partition number", zap.String("topic", o.topic), zap.Error(err))
	}
	if o.partitionNum == 0 {
		o.partitionNum = partitionNum
	}
	c.writer = w
	c.option = o
	return c
}

func (c *KafkaGoConsumer) Consume(ctx context.Context) error {
	topics := strings.Split(c.option.topic, ",")
	if len(topics) == 0 {
		log.Panic("Error no topics provided")
	}
	client, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      c.option.groupID,
		Brokers: c.option.address,
		Topics:  topics,
	})
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
	}

	defer func() {
		if err = client.Close(); err != nil {
			log.Panic("Error closing client", zap.Error(err))
		}
	}()

	for {
		gen, err := client.Next(context.Background())
		if err != nil {
			return err
		}
		for topic, assignments := range gen.Assignments {
			for _, assignment := range assignments {
				partition, offset := assignment.ID, assignment.Offset
				log.Info("start consume message",
					zap.String("topic", topic), zap.Int("partition", partition),
					zap.Int64("initialOffset", offset))
				// async read
				gen.Start(func(ctx context.Context) {
					reader := kafka.NewReader(kafka.ReaderConfig{
						Brokers:   c.option.address,
						Topic:     topic,
						Partition: partition,
					})
					defer reader.Close()

					// seek to the last committed offset for this partition.
					reader.SetOffset(offset)
					eventGroups := make(map[int64]*EventsGroup)
					for {
						msg, err := reader.ReadMessage(ctx)
						if err != nil {
							if errors.Is(err, kafka.ErrGenerationEnded) {
								// generation has ended.  commit offsets.  in a real app,
								// offsets would be committed periodically.
								gen.CommitOffsets(map[string]map[int]int64{topic: {partition: offset + 1}})
								return
							} else {
								log.Panic("Error reading message", zap.Error(err))
							}
						} else {
							if err := c.writer.Decode(ctx, c.option, int32(partition), msg.Key, msg.Value, eventGroups); err != nil {
								log.Panic("Error decode message", zap.Error(err))
							}
							// sync write to downstream
							if err := c.writer.Write(ctx); err != nil {
								log.Panic("Error write to downstream", zap.Error(err))
							}
							offset = msg.Offset
							gen.CommitOffsets(map[string]map[int]int64{topic: {partition: offset + 1}})
						}
					}
				})
			}
		}
	}
}

// async write to downsteam
func (c *KafkaGoConsumer) AsyncWrite(ctx context.Context) {
	c.writer.AsyncWrite(ctx)
}

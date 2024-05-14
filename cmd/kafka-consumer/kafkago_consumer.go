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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"time"

	cerror "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func newDialer(o *consumerOption) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   o.timeout,
		DualStack: true,
	}
	if len(o.ca) != 0 {
		caCert, err := os.ReadFile(o.ca)
		if err != nil {
			return nil, err
		}

		clientCert, err := tls.LoadX509KeyPair(o.cert, o.key)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		dialer.TLS = &tls.Config{
			RootCAs:            caCertPool,
			Certificates:       []tls.Certificate{clientCert},
			MinVersion:         tls.VersionTLS12,
			NextProtos:         []string{"h2", "http/1.1"},
			InsecureSkipVerify: false,
		}
	}
	return dialer, nil
}

func kafkaGoGetPartitionNum(o *consumerOption) (int32, error) {
	dialer, err := newDialer(o)
	if err != nil {
		log.Panic("Error create dialer")
	}
	client := &kafka.Client{
		Addr:      kafka.TCP(o.address...),
		Timeout:   o.timeout,
		Transport: &kafka.Transport{Dial: dialer.DialFunc},
	}
	for i := 0; i <= o.retryTime; i++ {
		resp, err := client.Metadata(context.Background(), &kafka.MetadataRequest{})
		if err != nil {
			return 0, cerror.Trace(err)
		}
		topics := resp.Topics
		for i := 0; i < len(topics); i++ {
			if topics[i].Name == o.topic {
				topicDetail := topics[i]
				numPartitions := int32(len(topicDetail.Partitions))
				log.Info("get partition number of topic",
					zap.String("topic", o.topic),
					zap.Int32("partitionNum", numPartitions))
				return numPartitions, nil
			}
		}
		log.Info("retry get partition number", zap.String("topic", o.topic))
		time.Sleep(1 * time.Second)
	}
	return 0, cerror.Errorf("wait the topic(%s) created timeout", o.topic)
}

type kafkaGoConsumer struct {
	option *consumerOption
	config kafka.ConsumerGroupConfig
	writer *writer
}

var _ KakfaConsumer = (*kafkaGoConsumer)(nil)

// NewKafkaGoConsumer will create a consumer client.
func NewKafkaGoConsumer(ctx context.Context, o *consumerOption) KakfaConsumer {
	c := new(kafkaGoConsumer)
	partitionNum, err := kafkaGoGetPartitionNum(o)
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
	dialer, err := newDialer(c.option)
	if err != nil {
		log.Panic("Error create dialer")
	}
	c.config = kafka.ConsumerGroupConfig{
		ID:      o.groupID,
		Brokers: o.address,
		Topics:  []string{o.topic},
		Dialer:  dialer,
	}
	return c
}

// Consume will read message from Kafka.
func (c *kafkaGoConsumer) Consume(ctx context.Context) error {
	client, err := kafka.NewConsumerGroup(c.config)
	if err != nil {
		log.Panic("Error creating consumer group client", zap.Error(err))
	}
	defer func() {
		if err = client.Close(); err != nil {
			log.Panic("Error closing client", zap.Error(err))
		}
	}()
	for {
		gen, err := client.Next(ctx)
		if err != nil {
			if err.(kafka.Error) == kafka.GroupCoordinatorNotAvailable {
				log.Warn("wait group coordinator available", zap.Error(err))
				continue
			}
			return err
		}
		for topic, assignments := range gen.Assignments {
			for _, assignment := range assignments {
				partition, offset := assignment.ID, assignment.Offset
				log.Info("start consume message",
					zap.String("topic", topic), zap.Int("partition", partition),
					zap.Int64("initialOffset", offset))
				gen.Start(func(ctx context.Context) {
					reader := kafka.NewReader(kafka.ReaderConfig{
						Brokers:   c.option.address,
						Topic:     topic,
						Partition: partition,
					})
					defer reader.Close()

					// seek to the last committed offset for this partition.
					if err := reader.SetOffset(offset); err != nil {
						log.Panic("Error set offset", zap.Error(err))
					}

					for {
						msg, err := reader.ReadMessage(ctx)
						if err != nil {
							if errors.Is(err, kafka.ErrGenerationEnded) {
								// generation has ended.  commit offsets.
								// in a real app, offsets would be committed periodically.
								if err = gen.CommitOffsets(map[string]map[int]int64{topic: {partition: offset + 1}}); err != nil {
									log.Panic("Error commit offsets", zap.Error(err))
								}
								return
							}
							log.Panic("Error reading message", zap.Error(err))
						}
						needCommit, err := c.writer.Decode(ctx, c.option, int32(partition), msg.Key, msg.Value)
						if err != nil {
							log.Panic("Error decode message", zap.Error(err))
						}
						offset = msg.Offset
						if needCommit {
							if err = gen.CommitOffsets(map[string]map[int]int64{topic: {partition: offset + 1}}); err != nil {
								log.Panic("Error commit offsets", zap.Error(err))
							}
						}
					}
				})
			}
		}
	}
}

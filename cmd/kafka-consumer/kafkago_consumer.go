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
	"strings"
	"time"

	cerror "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var (
	retryTime = 30
	timeout   = time.Second * 10
)

func newDialer(o *consumerOption) (*kafka.Dialer, error) {
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

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			RootCAs:            caCertPool,
			Certificates:       []tls.Certificate{clientCert},
			MinVersion:         tls.VersionTLS12,
			NextProtos:         []string{"h2", "http/1.1"},
			InsecureSkipVerify: false,
		},
	}
	return dialer, nil
}

func kafkaGoGetPartitionNum(o *consumerOption) (int32, error) {
	transport := kafka.DefaultTransport
	if len(o.ca) != 0 {
		if tlsDialer, err := newDialer(o); err == nil {
			transport = &kafka.Transport{Dial: tlsDialer.DialFunc}
		}
	}
	client := &kafka.Client{
		Addr:      kafka.TCP(o.address...),
		Timeout:   timeout,
		Transport: transport,
	}
	for i := 0; i <= retryTime; i++ {
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
	writer *writer
}

var _ KakfaConsumer = (*kafkaGoConsumer)(nil)

// NewkafkaGoConsumer will create a consumer client.
func NewkafkaGoConsumer(ctx context.Context, o *consumerOption) KakfaConsumer {
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
	return c
}

// Consume will read message from Kafka.
func (c *kafkaGoConsumer) Consume(ctx context.Context) error {
	topics := strings.Split(c.option.topic, ",")
	if len(topics) == 0 {
		log.Panic("Error no topics provided")
	}
	dialer := kafka.DefaultDialer
	if len(c.option.ca) != 0 {
		tlsDialer, err := newDialer(c.option)
		if err != nil {
			log.Panic("Error create dialer")
		}
		dialer = tlsDialer
	}
	client, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      c.option.groupID,
		Brokers: c.option.address,
		Topics:  topics,
		Dialer:  dialer,
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
					if err := reader.SetOffset(offset); err != nil {
						log.Panic("Error set offset", zap.Error(err))
					}

					// create new decoder
					decoder, err := NewDecoder(ctx, c.option, c.writer.upstreamTiDB)
					if err != nil {
						log.Panic("Error create decoder", zap.Error(err))
					}
					eventGroups := make(map[int64]*eventsGroup)
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
						if err := c.writer.Decode(decoder, c.option, int32(partition), msg.Key, msg.Value, eventGroups); err != nil {
							log.Panic("Error decode message", zap.Error(err))
						}
						// sync write to downstream
						if err := c.writer.Write(ctx); err != nil {
							log.Panic("Error write to downstream", zap.Error(err))
						}
						offset = msg.Offset
						if err = gen.CommitOffsets(map[string]map[int]int64{topic: {partition: offset + 1}}); err != nil {
							log.Panic("Error commit offsets", zap.Error(err))
						}
					}
				})
			}
		}
	}
}

// AsyncWrite call writer to write to the downsteam asynchronously.
func (c *kafkaGoConsumer) AsyncWrite(ctx context.Context) {
	if err := c.writer.AsyncWrite(ctx); err != nil {
		log.Info("async write break", zap.Error(err))
	}
}

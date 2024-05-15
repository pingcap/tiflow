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
	"os"
	"strings"
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
	return c
}

// Consume will read message from Kafka.
func (c *kafkaGoConsumer) Consume(ctx context.Context) error {
	topics := strings.Split(c.option.topic, ",")
	if len(topics) == 0 {
		log.Panic("Error no topics provided")
	}
	dialer, err := newDialer(c.option)
	if err != nil {
		log.Panic("Error create dialer")
	}
	client := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.option.address,
		GroupID:     c.option.groupID,
		GroupTopics: topics,
		Dialer:      dialer,
	})
	defer func() {
		if err = client.Close(); err != nil {
			log.Panic("Error closing client", zap.Error(err))
		}
	}()

	for {
		msg, err := client.FetchMessage(ctx)
		if err != nil {
			log.Error("Error fetch message", zap.Error(err))
		}
		needCommit, err := c.writer.Decode(ctx, c.option, int32(msg.Partition), msg.Key, msg.Value)
		if err != nil {
			log.Panic("Error decode message", zap.Error(err))
		}
		if needCommit {
			if err := client.CommitMessages(ctx, msg); err != nil {
				log.Error("Error commit message", zap.Error(err))
			}
		}
	}
}

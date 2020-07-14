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

package pulsar

import (
	"context"
	"net/url"
	"strconv"

	"github.com/apache/pulsar-client-go/pulsar"
)

// NewProducer create a pulsar producer.
func NewProducer(u *url.URL, errCh chan error) (*Producer, error) {
	opt, err := parseSinkOptions(u)
	if err != nil {
		return nil, err
	}
	client, err := pulsar.NewClient(*opt.clientOptions)
	if err != nil {
		return nil, err
	}
	producer, err := client.CreateProducer(*opt.producerOptions)
	if err != nil {
		client.Close()
		return nil, err
	}
	partitions, err := client.TopicPartitions(opt.producerOptions.Topic)
	if err != nil {
		client.Close()
		return nil, err
	}
	return &Producer{
		errCh:      errCh,
		opt:        *opt,
		client:     client,
		producer:   producer,
		partitions: len(partitions),
	}, nil
}

// Producer is a pulsar producer
type Producer struct {
	opt        Option
	client     pulsar.Client
	producer   pulsar.Producer
	errCh      chan error
	partitions int
}

// SendMessage implements the Producer interface
func (p *Producer) SendMessage(ctx context.Context, key []byte, value []byte, partition int32) error {
	p.producer.SendAsync(ctx, &pulsar.ProducerMessage{
		Payload:    value,
		Key:        string(key),
		Properties: map[string]string{route: strconv.Itoa(int(partition))},
	}, p.errors)
	return nil
}

func (p *Producer) errors(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
	if err != nil {
		select {
		case p.errCh <- err:
		default:
		}
	}
}

// SyncBroadcastMessage implements the Producer interface
func (p *Producer) SyncBroadcastMessage(ctx context.Context, key []byte, value []byte) error {
	for i := 0; i < p.partitions; i++ {
		_, err := p.producer.Send(ctx, &pulsar.ProducerMessage{
			Payload:    value,
			Key:        string(key),
			Properties: map[string]string{route: strconv.Itoa(i)},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Flush implements the Producer interface
func (p *Producer) Flush(_ context.Context) error {
	return p.producer.Flush()
}

// GetPartitionNum implements the Producer interface
func (p *Producer) GetPartitionNum() int32 {
	return int32(p.partitions)
}

// Close implements the Producer interface
func (p *Producer) Close() error {
	err := p.producer.Flush()
	if err != nil {
		return err
	}
	p.producer.Close()
	p.client.Close()
	return nil
}

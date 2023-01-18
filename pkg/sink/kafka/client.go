// Copyright 2022 PingCAP, Inc.
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

package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/rcrowley/go-metrics"
)

// Client is a generic Kafka client.
type Client interface {
	// Topics returns the set of available
	// topics as retrieved from cluster metadata.
	Topics() ([]string, error)
	// Partitions returns the sorted list of
	// all partition IDs for the given topic.
	Partitions(topic string) ([]int32, error)
	SyncProducer() (SyncProducer, error)
	AsyncProducer() (AsyncProducer, error)
	MetricRegistry() metrics.Registry
	// Close closes the client
	Close() error
}

// SyncProducer is the kafka sync producer
type SyncProducer interface {
	// SendMessage produces a given message, and returns only when it either has
	// succeeded or failed to produce. It will return the partition and the offset
	// of the produced message, or an error if the message failed to produce.
	SendMessage(topic string, partitionNum int32,
		key []byte, value []byte) error

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	SendMessages(topic string, partitionNum int32,
		key []byte, value []byte) error

	// Close shuts down the producer; you must call this function before a producer
	// object passes out of scope, as it may otherwise leak memory.
	// You must call this before calling Close on the underlying client.
	Close() error
}

// AsyncProducer is the kafka async producer
type AsyncProducer interface {
	// AsyncClose triggers a shutdown of the producer. The shutdown has completed
	// when both the Errors and Successes channels have been closed. When calling
	// AsyncClose, you *must* continue to read from those channels in order to
	// drain the results of any messages in flight.
	AsyncClose()

	// Close shuts down the producer and waits for any buffered messages to be
	// flushed. You must call this function before a producer object passes out of
	// scope, as it may otherwise leak memory. You must call this before process
	// shutting down, or you may lose messages. You must call this before calling
	// Close on the underlying client.
	Close() error

	// Input is the input channel for the user to write messages to that they
	// wish to send.
	Input() chan<- *sarama.ProducerMessage

	// Successes is the success output channel back to the user when Return.Successes is
	// enabled. If Return.Successes is true, you MUST read from this channel or the
	// Producer will deadlock. It is suggested that you send and read messages
	// together in a single select statement.
	Successes() <-chan *sarama.ProducerMessage

	// Errors is the error output channel back to the user. You MUST read from this
	// channel or the Producer will deadlock when the channel is full. Alternatively,
	// you can set Producer.Return.Errors in your config to false, which prevents
	// errors to be returned.
	Errors() <-chan *sarama.ProducerError
}

type saramaKafkaClient struct {
	client sarama.Client
}

func (c *saramaKafkaClient) Topics() ([]string, error) {
	return c.client.Topics()
}

// Partitions returns the sorted list of
// all partition IDs for the given topic.
func (c *saramaKafkaClient) Partitions(topic string) ([]int32, error) {
	return c.client.Partitions(topic)
}

func (c *saramaKafkaClient) SyncProducer() (SyncProducer, error) {
	p, err := sarama.NewSyncProducerFromClient(c.client)
	if err != nil {
		return nil, err
	}
	return &saramaSyncProducer{producer: p}, nil
}

func (c *saramaKafkaClient) AsyncProducer() (AsyncProducer, error) {
	p, err := sarama.NewAsyncProducerFromClient(c.client)
	if err != nil {
		return nil, err
	}
	return &saramaAsyncProducer{producer: p}, nil
}

func (c *saramaKafkaClient) MetricRegistry() metrics.Registry {
	return c.client.Config().MetricRegistry
}

func (c *saramaKafkaClient) Close() error {
	return c.client.Close()
}

type saramaSyncProducer struct {
	producer sarama.SyncProducer
}

func (p *saramaSyncProducer) SendMessage(topic string,
	partitionNum int32, key []byte, value []byte,
) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Partition: partitionNum,
	})
	return err
}

func (p *saramaSyncProducer) SendMessages(topic string,
	partitionNum int32, key []byte, value []byte,
) error {
	msgs := make([]*sarama.ProducerMessage, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		msgs[i] = &sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.ByteEncoder(key),
			Value:     sarama.ByteEncoder(value),
			Partition: int32(i),
		}
	}
	return p.producer.SendMessages(msgs)
}

func (p *saramaSyncProducer) Close() error {
	return p.producer.Close()
}

type saramaAsyncProducer struct {
	producer sarama.AsyncProducer
}

func (p *saramaAsyncProducer) AsyncClose() {
	p.producer.AsyncClose()
}

func (p *saramaAsyncProducer) Close() error {
	return p.producer.Close()
}

// Input is the input channel for the user to write messages to that they
// wish to send.
func (p *saramaAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return p.producer.Input()
}

func (p *saramaAsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return p.producer.Successes()
}

func (p *saramaAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return p.producer.Errors()
}

// ClientCreator defines the type of client crater.
type ClientCreator func(context.Context, *Options) (Client, error)

// NewSaramaClient constructs a Client with sarama.
func NewSaramaClient(ctx context.Context, o *Options) (Client, error) {
	saramaConfig, err := NewSaramaConfig(ctx, o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c, err := sarama.NewClient(o.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, err
	}
	return &saramaKafkaClient{client: c}, nil
}

// NewMockClient constructs a Client with mock implementation.
func NewMockClient(_ context.Context, _ *Options) (Client, error) {
	return NewClientMockImpl(), nil
}

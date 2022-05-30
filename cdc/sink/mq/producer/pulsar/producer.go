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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// NewProducer create a pulsar producer.
func NewProducer(u *url.URL, errCh chan error) (*Producer, error) {
	failpoint.Inject("MockPulsar", func() {
		failpoint.Return(&Producer{
			errCh:        errCh,
			partitionNum: 4,
		}, nil)
	})

	opt, err := parseSinkOptions(u)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}
	client, err := pulsar.NewClient(*opt.clientOptions)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}
	producer, err := client.CreateProducer(*opt.producerOptions)
	if err != nil {
		client.Close()
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}
	partitions, err := client.TopicPartitions(opt.producerOptions.Topic)
	if err != nil {
		client.Close()
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}
	return &Producer{
		errCh:        errCh,
		opt:          *opt,
		client:       client,
		producer:     producer,
		partitionNum: len(partitions),
	}, nil
}

// Producer provide a way to send msg to pulsar.
type Producer struct {
	opt          Option
	client       pulsar.Client
	producer     pulsar.Producer
	errCh        chan error
	partitionNum int
}

func createProperties(message *codec.MQMessage, partition int32) map[string]string {
	properties := map[string]string{route: strconv.Itoa(int(partition))}
	properties["ts"] = strconv.FormatUint(message.Ts, 10)
	properties["type"] = strconv.Itoa(int(message.Type))
	properties["protocol"] = strconv.Itoa(int(message.Protocol))
	if message.Schema != nil {
		properties["schema"] = *message.Schema
	}
	if message.Table != nil {
		properties["table"] = *message.Table
	}
	return properties
}

// AsyncSendMessage send key-value msg to target partition.
func (p *Producer) AsyncSendMessage(
	ctx context.Context, _ string, partition int32, message *codec.MQMessage,
) error {
	p.producer.SendAsync(ctx, &pulsar.ProducerMessage{
		Payload:    message.Value,
		Key:        string(message.Key),
		Properties: createProperties(message, partition),
		EventTime:  message.PhysicalTime(),
	}, p.errors)
	return nil
}

func (p *Producer) errors(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
	if err != nil {
		select {
		case p.errCh <- cerror.WrapError(cerror.ErrPulsarSendMessage, err):
		default:
			log.Error("error channel is full", zap.Error(err))
		}
	}
}

// SyncBroadcastMessage send key-value msg to all partition.
func (p *Producer) SyncBroadcastMessage(
	ctx context.Context, _ string, _ int32, message *codec.MQMessage,
) error {
	for partition := 0; partition < p.partitionNum; partition++ {
		_, err := p.producer.Send(ctx, &pulsar.ProducerMessage{
			Payload:    message.Value,
			Key:        string(message.Key),
			Properties: createProperties(message, int32(partition)),
			EventTime:  message.PhysicalTime(),
		})
		if err != nil {
			return cerror.WrapError(cerror.ErrPulsarSendMessage, p.producer.Flush())
		}
	}
	return nil
}

// Flush flushes all in memory msgs to server.
func (p *Producer) Flush(_ context.Context) error {
	return cerror.WrapError(cerror.ErrPulsarSendMessage, p.producer.Flush())
}

// GetPartitionNum got current topic's partition size.
func (p *Producer) GetPartitionNum() int32 {
	return int32(p.partitionNum)
}

// Close closes the producer and client.
func (p *Producer) Close() error {
	err := p.producer.Flush()
	if err != nil {
		return cerror.WrapError(cerror.ErrPulsarSendMessage, err)
	}
	p.producer.Close()
	p.client.Close()
	return nil
}

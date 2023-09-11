// Copyright 2023 PingCAP, Inc.
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

package ddlproducer

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

// Assert DDLEventSink implementation
var _ DDLProducer = (*PulsarMockProducers)(nil)

// PulsarMockProducers is a mock pulsar producer
type PulsarMockProducers struct {
	events map[string][]*pulsar.ProducerMessage
}

// SyncBroadcastMessage pulsar consume all partitions
func (p *PulsarMockProducers) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *common.Message,
) error {
	return p.SyncSendMessage(ctx, topic, totalPartitionsNum, message)
}

// SyncSendMessage sends a message
// partitionNum is not used,pulsar consume all partitions
func (p *PulsarMockProducers) SyncSendMessage(ctx context.Context, topic string,
	partitionNum int32, message *common.Message,
) error {
	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}
	p.events[topic] = append(p.events[topic], data)

	return nil
}

// NewMockPulsarProducer creates a pulsar producer
func NewMockPulsarProducer(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	pConfig *config.PulsarConfig,
	client pulsar.Client,
) (*PulsarMockProducers, error) {
	return &PulsarMockProducers{
		events: map[string][]*pulsar.ProducerMessage{},
	}, nil
}

// NewMockPulsarProducerDDL creates a pulsar producer for DDLProducer
func NewMockPulsarProducerDDL(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	pConfig *config.PulsarConfig,
	client pulsar.Client,
	sinkConfig *config.SinkConfig,
) (DDLProducer, error) {
	return NewMockPulsarProducer(ctx, changefeedID, pConfig, client)
}

// GetProducerByTopic returns a producer by topic name
func (p *PulsarMockProducers) GetProducerByTopic(topicName string) (producer pulsar.Producer, err error) {
	return producer, nil
}

// Close close all producers
func (p *PulsarMockProducers) Close() {
	p.events = make(map[string][]*pulsar.ProducerMessage)
}

// Flush waits for all the messages in the async producer to be sent to Pulsar.
// Notice: this method is not thread-safe.
// Do not try to call AsyncSendMessage and Flush functions in different threads,
// otherwise Flush will not work as expected. It may never finish or flush the wrong message.
// Because inflight will be modified by mistake.
func (p *PulsarMockProducers) Flush(ctx context.Context) error {
	return nil
}

// GetAllEvents returns the events received by the mock producer.
func (p *PulsarMockProducers) GetAllEvents() []*pulsar.ProducerMessage {
	var events []*pulsar.ProducerMessage
	for _, v := range p.events {
		events = append(events, v...)
	}
	return events
}

// GetEvents returns the event filtered by the key.
func (p *PulsarMockProducers) GetEvents(topic string) []*pulsar.ProducerMessage {
	return p.events[topic]
}

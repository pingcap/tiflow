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

package dmlproducer

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

var _ DMLProducer = (*pulsarDMLProducerMock)(nil)

// pulsarDMLProducer is used to send messages to kafka.
type pulsarDMLProducerMock struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// We hold the client to make close operation faster.
	// Please see the comment of Close().
	client pulsar.Client
	// producers is used to send messages to kafka asynchronously.
	// support multiple topics
	producers *lru.Cache

	// failpointCh is used to inject failpoints to the run loop.
	// Only used in test.
	failpointCh chan error
	// closeCh is send error
	errChan chan error

	pConfig *config.PulsarConfig
}

// NewPulsarDMLProducerMock creates a new pulsar producer.
func NewPulsarDMLProducerMock(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	client pulsar.Client,
	sinkConfig *config.SinkConfig,
	errCh chan error,
	failpointCh chan error,
) (DMLProducer, error) {
	pulsarConfig := sinkConfig.PulsarConfig
	defaultTopicName := pulsarConfig.GetDefaultTopicName()
	defaultProducer, err := newProducerMock(pulsarConfig, client, defaultTopicName)
	if err != nil {
		// Close the client to prevent the goroutine leak.
		// Because it may be a long time to close the client,
		// so close it asynchronously.
		// follow kafka
		go func() {
			client.Close()
		}()
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}
	producerCacheSize := config.DefaultPulsarProducerCacheSize
	if pulsarConfig != nil && pulsarConfig.PulsarProducerCacheSize != nil {
		producerCacheSize = int(*pulsarConfig.PulsarProducerCacheSize)
	}

	producers, err := lru.NewWithEvict(producerCacheSize, func(key interface{}, value interface{}) {
		// remove producer
		pulsarProducer, ok := value.(pulsar.Producer)
		if ok && pulsarProducer != nil {
			pulsarProducer.Close()
		}
	})
	if err != nil {
		go client.Close()
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}

	producers.Add(defaultTopicName, defaultProducer)

	p := &pulsarDMLProducerMock{
		id:          changefeedID,
		client:      client,
		producers:   producers,
		pConfig:     pulsarConfig,
		failpointCh: failpointCh,
		errChan:     errCh,
	}

	return p, nil
}

// AsyncSendMessage  Async send one message
func (p *pulsarDMLProducerMock) AsyncSendMessage(
	ctx context.Context, topic string,
	partition int32, message *common.Message,
) error {
	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}

	fmt.Printf("pulsar send message %+v\n", data)

	return nil
}

func (p *pulsarDMLProducerMock) Close() {
}

// newProducerMock creates a pulsar producer mock
func newProducerMock(
	pConfig *config.PulsarConfig,
	client pulsar.Client,
	topicName string,
) (p pulsar.Producer, err error) {
	return p, nil
}

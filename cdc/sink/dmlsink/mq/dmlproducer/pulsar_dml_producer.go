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
	"encoding/json"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics/mq"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

var _ DMLProducer = (*pulsarDMLProducer)(nil)

// pulsarDMLProducer is used to send messages to pulsar.
type pulsarDMLProducer struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// We hold the client to make close operation faster.
	// Please see the comment of Close().
	client pulsar.Client
	// producers is used to send messages to pulsar.
	// One topic only use one producer , so we want to have many topics but use less memory,
	// lru is a good idea to solve this question.
	// support multiple topics
	producers *lru.Cache

	// closedMu is used to protect `closed`.
	// We need to ensure that closed producers are never written to.
	closedMu sync.RWMutex
	// closed is used to indicate whether the producer is closed.
	// We also use it to guard against double closes.
	closed bool

	// failpointCh is used to inject failpoints to the run loop.
	// Only used in test.
	failpointCh chan error
	// closeCh is send error
	errChan chan error

	pConfig *config.PulsarConfig
}

// NewPulsarDMLProducer creates a new pulsar producer.
func NewPulsarDMLProducer(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	client pulsar.Client,
	sinkConfig *config.SinkConfig,
	errCh chan error,
	failpointCh chan error,
) (DMLProducer, error) {
	log.Info("Creating pulsar DML producer ...",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID))
	start := time.Now()

	var pulsarConfig *config.PulsarConfig
	if sinkConfig.PulsarConfig == nil {
		log.Error("new pulsar DML producer fail,sink:pulsar config is empty")
		return nil, cerror.ErrPulsarInvalidConfig.
			GenWithStackByArgs("pulsar config is empty")
	}

	pulsarConfig = sinkConfig.PulsarConfig
	defaultTopicName := pulsarConfig.GetDefaultTopicName()
	defaultProducer, err := newProducer(pulsarConfig, client, defaultTopicName)
	if err != nil {
		go client.Close()
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}
	producerCacheSize := config.DefaultPulsarProducerCacheSize
	if pulsarConfig != nil && pulsarConfig.PulsarProducerCacheSize != nil {
		producerCacheSize = int(*pulsarConfig.PulsarProducerCacheSize)
	}

	producers, err := lru.NewWithEvict(producerCacheSize, func(key interface{}, value interface{}) {
		// this is call when lru Remove producer or auto remove producer
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

	p := &pulsarDMLProducer{
		id:          changefeedID,
		client:      client,
		producers:   producers,
		pConfig:     pulsarConfig,
		closed:      false,
		failpointCh: failpointCh,
		errChan:     errCh,
	}
	log.Info("Pulsar DML producer created", zap.Stringer("changefeed", p.id),
		zap.Duration("duration", time.Since(start)))
	return p, nil
}

// AsyncSendMessage  Async send one message
func (p *pulsarDMLProducer) AsyncSendMessage(
	ctx context.Context, topic string,
	partition int32, message *common.Message,
) error {
	wrapperSchemaAndTopic(message)

	// We have to hold the lock to avoid writing to a closed producer.
	// Close may be blocked for a long time.
	p.closedMu.RLock()
	defer p.closedMu.RUnlock()

	// If producers are closed, we should skip the message and return an error.
	if p.closed {
		return cerror.ErrPulsarProducerClosed.GenWithStackByArgs()
	}
	failpoint.Inject("PulsarSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Pulsar meets error
		log.Info("PulsarSinkAsyncSendError error injected", zap.String("namespace", p.id.Namespace),
			zap.String("changefeed", p.id.ID))
		p.failpointCh <- errors.New("pulsar sink injected error")
		failpoint.Return(nil)
	})
	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}

	producer, err := p.GetProducerByTopic(topic)
	if err != nil {
		return err
	}

	// if for stress test record , add count to message callback function

	producer.SendAsync(ctx, data,
		func(id pulsar.MessageID, m *pulsar.ProducerMessage, err error) {
			// fail
			if err != nil {
				e := cerror.WrapError(cerror.ErrPulsarAsyncSendMessage, err)
				log.Error("Pulsar DML producer async send error",
					zap.String("namespace", p.id.Namespace),
					zap.String("changefeed", p.id.ID),
					zap.Error(err))
				mq.IncPublishedDMLFail(topic, p.id.ID, message.GetSchema())
				// use this select to avoid send error to a closed channel
				// the ctx will always be called before the errChan is closed
				select {
				case <-ctx.Done():
					return
				case p.errChan <- e:
				default:
					log.Warn("Error channel is full in pulsar DML producer",
						zap.Stringer("changefeed", p.id), zap.Error(e))
				}
			} else if message.Callback != nil {
				// success
				message.Callback()
				mq.IncPublishedDMLSuccess(topic, p.id.ID, message.GetSchema())
			}
		})

	mq.IncPublishedDMLCount(topic, p.id.ID, message.GetSchema())

	return nil
}

func (p *pulsarDMLProducer) Close() { // We have to hold the lock to synchronize closing with writing.
	p.closedMu.Lock()
	defer p.closedMu.Unlock()
	// If the producer has already been closed, we should skip this close operation.
	if p.closed {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		log.Warn("Pulsar DML producer already closed",
			zap.String("namespace", p.id.Namespace),
			zap.String("changefeed", p.id.ID))
		return
	}
	close(p.failpointCh)
	p.closed = true
	start := time.Now()
	keys := p.producers.Keys()
	for _, topic := range keys {
		p.producers.Remove(topic) // callback func will be called
		topicName, _ := topic.(string)
		log.Info("Async client closed in pulsar DML producer",
			zap.Duration("duration", time.Since(start)),
			zap.String("namespace", p.id.Namespace),
			zap.String("changefeed", p.id.ID), zap.String("topic", topicName))
	}
	p.client.Close()
}

// newProducer creates a pulsar producer
// One topic is used by one producer
func newProducer(
	pConfig *config.PulsarConfig,
	client pulsar.Client,
	topicName string,
) (pulsar.Producer, error) {
	maxReconnectToBroker := uint(config.DefaultMaxReconnectToPulsarBroker)
	option := pulsar.ProducerOptions{
		Topic:                topicName,
		MaxReconnectToBroker: &maxReconnectToBroker,
	}
	if pConfig.BatchingMaxMessages != nil {
		option.BatchingMaxMessages = *pConfig.BatchingMaxMessages
	}
	if pConfig.BatchingMaxPublishDelay != nil {
		option.BatchingMaxPublishDelay = pConfig.BatchingMaxPublishDelay.Duration()
	}
	if pConfig.CompressionType != nil {
		option.CompressionType = pConfig.CompressionType.Value()
		option.CompressionLevel = pulsar.Default
	}
	if pConfig.SendTimeout != nil {
		option.SendTimeout = pConfig.SendTimeout.Duration()
	}

	producer, err := client.CreateProducer(option)
	if err != nil {
		return nil, err
	}

	log.Info("create pulsar producer success", zap.String("topic", topicName))

	return producer, nil
}

func (p *pulsarDMLProducer) getProducer(topic string) (pulsar.Producer, bool) {
	target, ok := p.producers.Get(topic)
	if ok {
		producer, ok := target.(pulsar.Producer)
		if ok {
			return producer, true
		}
	}
	return nil, false
}

// GetProducerByTopic get producer by topicName,
// if not exist, it will create a producer with topicName, and set in LRU cache
// more meta info at pulsarDMLProducer's producers
func (p *pulsarDMLProducer) GetProducerByTopic(topicName string) (producer pulsar.Producer, err error) {
	getProducer, ok := p.getProducer(topicName)
	if ok && getProducer != nil {
		return getProducer, nil
	}

	if !ok { // create a new producer for the topicName
		producer, err = newProducer(p.pConfig, p.client, topicName)
		if err != nil {
			return nil, err
		}
		p.producers.Add(topicName, producer)
	}

	return producer, nil
}

// wrapperSchemaAndTopic wrapper schema and topic
func wrapperSchemaAndTopic(m *common.Message) {
	if m.Schema == nil {
		if m.Protocol == config.ProtocolMaxwell {
			mx := &maxwellMessage{}
			err := json.Unmarshal(m.Value, mx)
			if err != nil {
				log.Error("unmarshal maxwell message failed", zap.Error(err))
				return
			}
			if len(mx.Database) > 0 {
				m.Schema = &mx.Database
			}
			if len(mx.Table) > 0 {
				m.Table = &mx.Table
			}
		}
		if m.Protocol == config.ProtocolCanal { // canal protocol set multi schemas in one topic
			m.Schema = str2Pointer("multi_schema")
		}
	}
}

// maxwellMessage is the message format of maxwell
type maxwellMessage struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

// str2Pointer returns the pointer of the string.
func str2Pointer(str string) *string {
	return &str
}

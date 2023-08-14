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
	"encoding/json"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	"go.uber.org/zap"
)

const (
	// DefaultPulsarProducerCacheSize is the default size of the cache for producers
	// 10240 producers maybe cost 1.1G memory
	DefaultPulsarProducerCacheSize = 10240
)

// Assert DDLEventSink implementation
var _ DDLProducer = (*pulsarProducers)(nil)

// pulsarProducers is a producer for pulsar
type pulsarProducers struct {
	client           pulsar.Client
	pConfig          *pulsarConfig.Config
	defaultTopicName string
	// support multiple topics
	producers      *lru.Cache
	producersMutex sync.RWMutex
	id             model.ChangeFeedID
}

// SyncBroadcastMessage pulsar consume all partitions
func (p *pulsarProducers) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *common.Message,
) error {
	// call SyncSendMessage
	// pulsar consumer all partitions
	return p.SyncSendMessage(ctx, topic, totalPartitionsNum, message)
}

// SyncSendMessage sends a message
// partitionNum is not used,pulsar consume all partitions
func (p *pulsarProducers) SyncSendMessage(ctx context.Context, topic string,
	partitionNum int32, message *common.Message,
) error {
	p.wrapperSchemaAndTopic(message)

	producer, err := p.GetProducerByTopic(topic)
	if err != nil {
		log.Error("ddl SyncSendMessage GetProducerByTopic fail", zap.Error(err))
		return err
	}

	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}
	mID, err := producer.Send(ctx, data)
	if err != nil {
		log.Error("ddl producer send fail", zap.Error(err))
		return err
	}

	log.Debug("pulsarProducers SyncSendMessage success",
		zap.Any("mID", mID), zap.String("topic", topic))

	return nil
}

// NewPulsarProducer creates a pulsar producer
func NewPulsarProducer(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	pConfig *pulsarConfig.Config,
	client pulsar.Client,
	sinkConfig *config.SinkConfig,
) (DDLProducer, error) {
	log.Info("Starting pulsar DDL producer ...",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID))

	topicName := pConfig.GetDefaultTopicName()

	defaultProducer, err := newProducer(pConfig, client, topicName)
	if err != nil {
		return nil, err
	}

	producerCacheSize := DefaultPulsarProducerCacheSize
	if sinkConfig.PulsarConfig != nil && sinkConfig.PulsarConfig.PulsarProducerCacheSize != nil {
		producerCacheSize = int(*sinkConfig.PulsarConfig.PulsarProducerCacheSize)
	}

	producers, err := lru.NewWithEvict(producerCacheSize, func(key interface{}, value interface{}) {
		// remove producer
		pulsarProducer, ok := value.(pulsar.Producer)
		if ok && pulsarProducer != nil {
			pulsarProducer.Close()
		}
	})
	if err != nil {
		return nil, err
	}

	producers.Add(topicName, defaultProducer)
	return &pulsarProducers{
		client:           client,
		pConfig:          pConfig,
		producers:        producers,
		defaultTopicName: topicName,
		id:               changefeedID,
	}, nil
}

// newProducer creates a pulsar producer
// One topic is used by one producer
func newProducer(
	pConfig *pulsarConfig.Config,
	client pulsar.Client,
	topicName string,
) (pulsar.Producer, error) {
	po := pulsar.ProducerOptions{
		Topic: topicName,
	}
	if pConfig.BatchingMaxMessages > 0 {
		po.BatchingMaxMessages = pConfig.BatchingMaxMessages
	}
	if pConfig.BatchingMaxPublishDelay > 0 {
		po.BatchingMaxPublishDelay = pConfig.BatchingMaxPublishDelay
	}
	if pConfig.CompressionType > 0 {
		po.CompressionType = pConfig.CompressionType
		po.CompressionLevel = pulsar.Default
	}
	if pConfig.SendTimeout > 0 {
		po.SendTimeout = pConfig.SendTimeout
	}

	producer, err := client.CreateProducer(po)
	if err != nil {
		return nil, err
	}

	log.Info("create pulsar producer success",
		zap.String("topic:", topicName))

	return producer, nil
}

func (p *pulsarProducers) getProducer(topic string) (pulsar.Producer, bool) {
	target, ok := p.producers.Get(topic)
	if ok {
		producer, ok := target.(pulsar.Producer)
		if ok {
			return producer, true
		}
	}
	return nil, false
}

// GetProducerByTopic get producer by topicName
func (p *pulsarProducers) GetProducerByTopic(topicName string) (producer pulsar.Producer, err error) {
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

// Close close all producers
func (p *pulsarProducers) Close() {
	keys := p.producers.Keys()
	p.producersMutex.Lock()
	defer p.producersMutex.Unlock()
	for _, topic := range keys {
		p.producers.Remove(topic) // callback func will be called
	}
}

// wrapperSchemaAndTopic wrapper schema and topic
func (p *pulsarProducers) wrapperSchemaAndTopic(m *common.Message) {
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

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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	// pulsarMetric "github.com/pingcap/tiflow/cdc/sink/metrics/mq/pulsar"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	"go.uber.org/zap"
)

// Assert DDLEventSink implementation
var _ DDLProducer = (*pulsarProducers)(nil)

// pulsarProducers is a producer for pulsar
type pulsarProducers struct {
	client           pulsar.Client
	pConfig          *pulsarConfig.Config
	defaultTopicName string
	// support multiple topics
	producers      map[string]pulsar.Producer
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

	// pulsarMetric.IncPublishedDDLEventCountMetric(topic, p.id.ID, message)
	producer, err := p.GetProducerByTopic(topic)
	if err != nil {
		log.L().Error("ddl SyncSendMessage GetProducerByTopic fail", zap.Error(err))
		// pulsarMetric.IncPublishedDDLEventCountMetricFail(topic, p.id.ID, message)
		return err
	}

	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}
	mID, err := producer.Send(ctx, data)
	if err != nil {
		log.L().Error("ddl producer send fail", zap.Error(err))
		//	pulsarMetric.IncPublishedDDLEventCountMetricFail(producer.Topic(), p.id.ID, message)
		return err
	}
	// pulsarMetric.IncPublishedDDLEventCountMetricSuccess(producer.Topic(), p.id.ID, message)

	log.L().Debug("pulsarProducers SyncSendMessage success",
		zap.Any("mID", mID), zap.String("topic", topic))

	return nil
}

// NewPulsarProducer creates a pulsar producer
func NewPulsarProducer(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	pConfig *pulsarConfig.Config,
	client pulsar.Client,
) (DDLProducer, error) {
	log.Info("Starting pulsar DDL producer ...",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID))

	topicName := pConfig.GetDefaultTopicName()

	defaultProducer, err := newProducer(pConfig, client, topicName)
	if err != nil {
		return nil, err
	}
	producers := make(map[string]pulsar.Producer)
	producers[topicName] = defaultProducer
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

	log.L().Info("create pulsar producer success",
		zap.String("topic:", topicName))

	return producer, nil
}

// GetProducerByTopic get producer by topicName
func (p *pulsarProducers) GetProducerByTopic(topicName string) (producer pulsar.Producer, err error) {
	p.producersMutex.RLock()
	producer, ok := p.producers[topicName]
	p.producersMutex.RUnlock()
	if !ok { // create a new producer for the topicName
		p.producersMutex.Lock()
		defer p.producersMutex.Unlock()

		producer, ok = p.producers[topicName]
		if ok {
			return producer, nil
		}

		producer, err = newProducer(p.pConfig, p.client, topicName)
		if err != nil {
			return nil, err
		}
		p.producers[topicName] = producer
	}

	return producer, nil
}

// Close close all producers
func (p *pulsarProducers) Close() {
	for topic, producer := range p.producers {
		producer.Close()
		p.closeProducersMapByTopic(topic)
	}
	p.client.Close()
}

// closeProducersMapByTopic close producer by topicName
func (p *pulsarProducers) closeProducersMapByTopic(topicName string) {
	p.producersMutex.Lock()
	defer p.producersMutex.Unlock()
	_, ok := p.producers[topicName]
	if ok {
		delete(p.producers, topicName)
		return
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

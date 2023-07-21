package ddlproducer

import (
	"context"
	"encoding/json"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	pulsarMetric "github.com/pingcap/tiflow/cdc/sinkv2/metrics/mq/pulsar"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
)

// Assert DDLEventSink implementation
var _ DDLProducer = (*pulsarProducers)(nil)

type pulsarProducers struct {
	client           pulsar.Client
	pConfig          *pulsarConfig.PulsarConfig
	defaultTopicName string
	// support multiple topics
	producers      map[string]pulsar.Producer
	producersMutex sync.RWMutex
	id             model.ChangeFeedID
}

// SyncBroadcastMessage pulsar consume all partitions
func (p *pulsarProducers) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *common.Message) error {
	// call SyncSendMessage
	return p.SyncSendMessage(ctx, topic, totalPartitionsNum, message)
}

// SyncSendMessage sends a message
// partitionNum is not used,pulsar consume all partitions
func (p *pulsarProducers) SyncSendMessage(ctx context.Context, topic string,
	partitionNum int32, message *common.Message) error {
	p.wrapperSchemaAndTopic(message)

	pulsarMetric.IncPublishedDDLEventCountMetric(topic, p.id.ID, message)
	producer, err := p.GetProducerByTopic(topic)
	if err != nil {
		log.L().Error("ddl SyncSendMessage GetProducerByTopic fail", zap.Error(err))
		pulsarMetric.IncPublishedDDLEventCountMetricFail(topic, p.id.ID, message)
		return err
	}

	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     p.pConfig.MessageKey,
	}
	mID, err := producer.Send(ctx, data)
	if err != nil {
		log.L().Error("ddl producer send fail", zap.Error(err))
		pulsarMetric.IncPublishedDDLEventCountMetricFail(producer.Topic(), p.id.ID, message)
		return err
	}
	pulsarMetric.IncPublishedDDLEventCountMetricSuccess(producer.Topic(), p.id.ID, message)

	log.L().Debug("pulsarProducers SyncSendMessage success",
		zap.Any("mID", mID), zap.String("topic", topic))

	return nil
}

// NewPulsarProducer creates a pulsar producer
func NewPulsarProducer(
	ctx context.Context,
	pConfig *pulsarConfig.PulsarConfig,
	client pulsar.Client,
) (DDLProducer, error) {

	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
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
	pConfig *pulsarConfig.PulsarConfig,
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
	if len(pConfig.Compression) > 0 {
		switch strings.ToLower(pConfig.Compression) {
		case "lz4":
			po.CompressionType = pulsar.LZ4
		case "zlib":
			po.CompressionType = pulsar.ZLib
		case "zstd":
			po.CompressionType = pulsar.ZSTD
		}
	}

	if pConfig.SendTimeout > 0 {
		po.SendTimeout = time.Millisecond * time.Duration(pConfig.SendTimeout)
	}

	switch pConfig.ProducerMode {
	case pulsarConfig.ProducerModeSingle:
		// set default message key '0'
		// if not set default value, will be random sent to multiple partitions
		if len(pConfig.MessageKey) == 0 {
			pConfig.MessageKey = "0"
		}
	case pulsarConfig.ProducerModeBatch, "":
		// default ,message key is empty
	}

	producer, err := client.CreateProducer(po)
	if err != nil {
		return nil, err
	}

	log.L().Info("create pulsar producer successfully",
		zap.String("topicName", topicName))

	return producer, nil
}

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

// Flush waits for all the messages in the async producer to be sent to Pulsar.
// Notice: this method is not thread-safe.
// Do not try to call AsyncSendMessage and Flush functions in different threads,
// otherwise Flush will not work as expected. It may never finish or flush the wrong message.
// Because inflight will be modified by mistake.
func (p *pulsarProducers) Flush(ctx context.Context) error {
	done := make(chan struct{}, 1)
	p.producersMutex.Lock()
	for _, pd := range p.producers {
		pd.Flush()
	}
	p.producersMutex.Unlock()
	done <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}

}

func (p *pulsarProducers) closeProducersMapByTopic(topicName string) error {

	p.producersMutex.Lock()
	defer p.producersMutex.Unlock()
	producer, _ := p.producers[topicName]
	if producer == nil {
		return nil
	}
	p.producers[topicName] = nil

	return nil
}

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

type maxwellMessage struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

func str2Pointer(str string) *string {
	return &str
}

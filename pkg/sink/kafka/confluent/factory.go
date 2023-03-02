package confluent

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	v2 "github.com/pingcap/tiflow/pkg/sink/kafka/v2"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type factory struct {
	options      *pkafka.Options
	changefeedID model.ChangeFeedID

	helper pkafka.Factory
}

func NewFactory(
	options *pkafka.Options,
	changefeedID model.ChangeFeedID,
) (pkafka.Factory, error) {
	helper, err := v2.NewFactory(options, changefeedID)
	if err != nil {
		return nil, err
	}

	return &factory{
		options:      options,
		changefeedID: changefeedID,
		helper:       helper,
	}, nil

}

func (f *factory) AdminClient() (pkafka.ClusterAdminClient, error) {
	return f.helper.AdminClient()
}

// SyncProducer creates a sync producer to writer message to kafka
func (f *factory) SyncProducer() (pkafka.SyncProducer, error) {
	return f.helper.SyncProducer()
}

// AsyncProducer creates an async producer to writer message to kafka
func (f *factory) AsyncProducer(
	ctx context.Context,
	closedChan chan struct{},
	failpointCh chan error,
) (pkafka.AsyncProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": f.options.BrokerEndpoints,
	})
	if err != nil {
		return nil, err
	}

	return &asyncProducer{
		producer:     producer,
		closedChan:   closedChan,
		failpointCh:  failpointCh,
		changefeedID: f.changefeedID,
	}, nil
}

// MetricsCollector returns the kafka metrics collector
func (f *factory) MetricsCollector(
	role util.Role,
	adminClient pkafka.ClusterAdminClient,
) pkafka.MetricsCollector {
	return f.helper.MetricsCollector(role, adminClient)
}

type asyncProducer struct {
	producer *kafka.Producer

	closedChan  chan struct{}
	failpointCh chan error
	//errorsChan   chan error
	changefeedID model.ChangeFeedID
}

func (a *asyncProducer) AsyncSend(ctx context.Context, topic string,
	partition int32, key []byte, value []byte,
	callback func()) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-a.closedChan:
		log.Warn("Receive from closed chan in kafka producer",
			zap.String("namespace", a.changefeedID.Namespace),
			zap.String("changefeed", a.changefeedID.ID))
		return nil
	default:
	}
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
		},
		Key:    key,
		Value:  value,
		Opaque: callback,
	}

	return a.producer.Produce(message, nil)
}

func (a *asyncProducer) AsyncRunCallback(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-a.closedChan:
			return nil
		case err := <-a.failpointCh:
			log.Warn("Receive from failpoint chan in kafka "+
				"DML producer",
				zap.String("namespace", a.changefeedID.Namespace),
				zap.String("changefeed", a.changefeedID.ID),
				zap.Error(err))
			return errors.Trace(err)
		case ev := <-a.producer.Events():
			switch e := ev.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					log.Warn("Delivery failed",
						zap.Error(e.TopicPartition.Error))
					return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, e.TopicPartition.Error)
				}
				if e.Opaque != nil {
					e.Opaque.(func())()
				}
			}
		}
	}
}

func (a *asyncProducer) Close() {
	a.producer.Close()
}

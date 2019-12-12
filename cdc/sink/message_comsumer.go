package sink

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"
)

type MessageConsumer struct {
	topic  string
	client sarama.ConsumerGroup

	cdcMessageMap sync.Map
	lock          sync.Mutex
	metaGroup     *sync.WaitGroup
}

func NewMessageConsumer(cfg KafkaConfig) (*MessageConsumer, error) {
	config, err := newSaramaConfig(cfg.KafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Retry.Backoff = 500 * time.Millisecond

	consumer, err := sarama.NewConsumerGroup(strings.Split(cfg.KafkaAddrs, ","), "", config)
	if err != nil {
		return nil, err
	}

	return &MessageConsumer{
		client: consumer,
	}, nil

}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (consumer *MessageConsumer) Start(ctx context.Context, topics string) {
	go func() {
		for {
			if err := consumer.client.Consume(ctx, strings.Split(topics, ","), consumer); err != nil {
				log.Error("Error from consumer", zap.Error(err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()
}

func (consumer *MessageConsumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *MessageConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

var aliveCDCAccount = 1
var cdcRSMap = map[string][]*Message{}

func decode(message *sarama.ConsumerMessage) *Message {
	return nil
}

func (consumer *MessageConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		msg := decode(message)

		switch msg.MsgType {
		case TxnType:
			messages, _ := cdcRSMap[msg.CdcID]
			messages = append(messages, msg)
			cdcRSMap[msg.CdcID] = messages
		case ResolveTsType:
			if len(cdcRSMap) >= aliveCDCAccount {

			}
			//save DML and DDL
		case MetaType: //cdc is added or deleted
			consumer.metaGroup.Done()
			consumer.metaGroup.Wait()
			aliveCDCAccount = len(msg.CdcList)
			existsMap := map[string]bool{}
			for _, cdcName := range msg.CdcList {
				existsMap[cdcName] = true
				//if _, found := cdcRSMap[cdcName]; !found {
				//	//cdc is added
				//	cdcRSMap[cdcName] = []*Message{}
				//}
			}
			for cdcName, _ := range cdcRSMap {
				if !existsMap[cdcName] {
					//cdc is deleted
					delete(cdcRSMap, cdcName)
				}
			}
		}
	}
	return nil
}

func (consumer *MessageConsumer) createWaitGroup(msg *Message) {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()

	if consumer.metaGroup == nil {
		consumer.metaGroup = &sync.WaitGroup{}
		consumer.metaGroup.Add(msg.MetaCount)
	}
}

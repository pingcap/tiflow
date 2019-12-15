// Copyright 2019 PingCAP, Inc.
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

package sink

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"go.uber.org/zap"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type offsetCommitter interface {
	MarkOffset(topic string, partition int32, offset int64, metadata string)
}

type messageConsumer struct {
	topic   string
	client  sarama.ConsumerGroup
	sink    Sink
	started bool

	cdcResolveTsMap     map[string][]*resolveMsgWrapper
	partitionMessageMap map[int32][]*decodedKafkaMessage
	tableInfoMap        map[int64]*timodel.TableInfo
	tableName2IdMap     map[string]int64

	lock       sync.Mutex
	metaGroup  *sync.WaitGroup
	cleanGroup *sync.WaitGroup
	cdcCount   int
}

type decodedKafkaMessage struct {
	partition int32
	offset    int64
	message   *Message
}

type resolveMsgWrapper struct {
	ResolveTs uint64
	partition int32
	offset    int64
}

func NewMessageConsumer(kafkaVersion, kafkaAddr, kafkaTopic string) (*messageConsumer, TableInfoGetter, error) {
	config, err := newSaramaConfig(kafkaVersion)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Retry.Backoff = 500 * time.Millisecond

	consumerGroup, err := sarama.NewConsumerGroup(strings.Split(kafkaAddr, ","), "", config)
	if err != nil {
		return nil, nil, err
	}

	consumer := &messageConsumer{
		client:              consumerGroup,
		topic:               kafkaTopic,
		cdcResolveTsMap:     map[string][]*resolveMsgWrapper{},
		partitionMessageMap: map[int32][]*decodedKafkaMessage{},
		tableInfoMap:        map[int64]*timodel.TableInfo{},
		tableName2IdMap:     map[string]int64{},
	}
	return consumer, consumer, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (consumer *messageConsumer) Start(ctx context.Context, sink Sink) error {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()

	if consumer.started {
		return errors.Errorf("kafka consumer is already started")
	}
	consumer.sink = sink
	go func() {
		for {
			if err := consumer.client.Consume(ctx, strings.Split(consumer.topic, ","), consumer); err != nil {
				log.Error("Error from consumer", zap.Error(err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

func (consumer *messageConsumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *messageConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *messageConsumer) TableByID(id int64) (info *timodel.TableInfo, ok bool) {
	info, ok = consumer.tableInfoMap[id]
	return
}

func (consumer *messageConsumer) GetTableIDByName(schema, table string) (id int64, ok bool) {
	id, ok = consumer.tableName2IdMap[FormMapKey(schema, table)]
	return
}

func (consumer *messageConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		msg, err := NewReader(message.Value).Decode()
		if err != nil {
			log.Fatal("failed to decode kafka message", zap.Error(err))
		}
		consumer.processMsg(message.Partition, message.Offset, msg, session)
	}
	return nil
}

func (consumer *messageConsumer) processMsg(partition int32, offset int64, msg *Message, committer offsetCommitter) {
	switch msg.MsgType {
	case TxnType:
		consumer.processTxnMsg(partition, offset, msg)
	case ResolveTsType:
		consumer.processResolveRSMsg(partition, offset, msg)
	case MetaType: //cdc is added or deleted
		consumer.processMetaMsg(committer, msg)()
	}
}

func (consumer *messageConsumer) processTxnMsg(partition int32, offset int64, msg *Message) {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()

	for key, v := range msg.TableInfos {
		consumer.tableInfoMap[v.ID] = v
		consumer.tableName2IdMap[key] = v.ID
	}

	wrapper := &decodedKafkaMessage{message: msg, partition: partition, offset: offset}
	messages, _ := consumer.partitionMessageMap[wrapper.partition]
	messages = append(messages, wrapper)
	consumer.partitionMessageMap[wrapper.partition] = messages
}
func (consumer *messageConsumer) processResolveRSMsg(partition int32, offset int64, msg *Message) {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()

	wrapper := &resolveMsgWrapper{ResolveTs: msg.ResloveTs, partition: partition, offset: offset}
	messages, _ := consumer.cdcResolveTsMap[msg.CdcID]
	messages = append(messages, wrapper)
	consumer.cdcResolveTsMap[msg.CdcID] = messages

	//add to partition cache too
	wrapper2 := &decodedKafkaMessage{message: msg, partition: partition, offset: offset}
	messages2, _ := consumer.partitionMessageMap[wrapper2.partition]
	messages2 = append(messages2, wrapper2)
	consumer.partitionMessageMap[wrapper.partition] = messages2
}

func (consumer *messageConsumer) processMetaMsg(committer offsetCommitter, msg *Message) func() {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()

	if consumer.metaGroup == nil {
		consumer.metaGroup = &sync.WaitGroup{}
		consumer.metaGroup.Add(len(msg.CdcList) - 1)
		consumer.cleanGroup = &sync.WaitGroup{}
		consumer.cleanGroup.Add(1)
		return func() {
			defer consumer.cleanGroup.Done()

			consumer.metaGroup.Wait()
			consumer.tryPersistent(committer)

			//after this time the cdc node count is changed
			consumer.cdcCount = len(msg.CdcList)
			existsMap := map[string]bool{}
			for _, cdcName := range msg.CdcList {
				existsMap[cdcName] = true
			}
			for cdcName, _ := range consumer.cdcResolveTsMap {
				if !existsMap[cdcName] {
					//cdc is deleted
					delete(consumer.cdcResolveTsMap, cdcName)
				}
			}
			consumer.metaGroup = nil
		}
	}
	return func() {
		consumer.metaGroup.Done()
		consumer.cleanGroup.Wait()
		consumer.cleanGroup = nil
	}
}

func (consumer *messageConsumer) tryPersistent(offsetCommitter offsetCommitter) {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()

	for {
		//check if we received all RS from all cdc node
		if consumer.cdcCount > 0 && consumer.cdcCount <= len(consumer.cdcResolveTsMap) {
			minRS, minRsCdcName, skip, offsetMap := consumer.findMinRs()
			if skip { //no enough rs data
				return
			}
			//find all DML and DDL that ts less than minRS
			txnMap := consumer.getTxnMap(minRS)
			//sort and save to MySQL
			consumer.saveMessage2Sink(txnMap, minRS)
			//commit kafka offset
			consumer.commitKafkaOffset(offsetMap, offsetCommitter)
			//delete saved rs
			consumer.cdcResolveTsMap[minRsCdcName] = consumer.cdcResolveTsMap[minRsCdcName][1:]
			//delete saved messages
			consumer.deleteSaveKafkaMessage(minRS)
		} else {
			break
		}
	}
}

func (consumer *messageConsumer) calCommitOffset(minRS uint64) map[int32]int64 {
	offsetMap := map[int32]int64{}
	for partition, messages := range consumer.partitionMessageMap {
		for _, msg := range messages {
			if msg.message.MsgType == ResolveTsType && msg.message.ResloveTs == minRS {
				offsetMap[partition] = msg.offset
			}
		}
	}
	return offsetMap
}

func (consumer *messageConsumer) getTxnMap(minRS uint64) map[uint64][]*Message {
	txnMap := map[uint64][]*Message{}
	for _, messages := range consumer.partitionMessageMap {
		for _, msg := range messages {
			if msg.message.MsgType == TxnType {
				if msg.message.Txn.Ts <= minRS {
					txnMessages := txnMap[msg.message.Txn.Ts]
					txnMessages = append(txnMessages, msg.message)
					txnMap[msg.message.Txn.Ts] = txnMessages
				}
			}
		}
	}
	return txnMap
}

func (consumer *messageConsumer) findMinRs() (uint64, string, bool, map[int32]int64) {
	minRS := uint64(math.MaxUint64)
	offsetMap := map[int32]int64{}
	minRsCdcName := ""
	for cdcName, messages := range consumer.cdcResolveTsMap {
		if len(messages) <= 0 { //has no rs, we can not calculate the min rs, skip
			return 0, "", true, nil
		}
		if messages[0].ResolveTs < minRS {
			minRS = messages[0].ResolveTs
			minRsCdcName = cdcName
			offsetMap[messages[0].partition] = messages[0].offset
		}
	}
	return minRS, minRsCdcName, false, offsetMap
}

func (consumer *messageConsumer) saveMessage2Sink(txnMap map[uint64][]*Message, minRS uint64) {
	list := TxnSlice{}
	for key, v := range txnMap {
		list = append(list, Txn{ts: key, msgs: v})
	}
	sort.Sort(list)
	for _, item := range list {
		//save to sink
		for _, txn := range item.msgs {
			//todo: error handle
			if err := consumer.sink.Emit(context.Background(), *txn.Txn); err != nil {
				log.Fatal("save to sink failed", zap.Error(err))
			}
		}
	}
	if err := consumer.sink.EmitResolvedTimestamp(context.Background(), minRS); err != nil {
		log.Fatal("save to sink failed", zap.Error(err))
	}
}

func (consumer *messageConsumer) commitKafkaOffset(offsetMap map[int32]int64, committer offsetCommitter) {
	for partition, offset := range offsetMap {
		committer.MarkOffset(consumer.topic, partition, offset, "")
	}
}

func (consumer *messageConsumer) deleteSaveKafkaMessage(minRS uint64) {
	for partition, list := range consumer.partitionMessageMap {
		n := 0
		for _, item := range list {
			if (item.message.MsgType == ResolveTsType && item.message.ResloveTs <= minRS) ||
				(item.message.MsgType == TxnType && item.message.Txn.Ts > minRS) {
				list[n] = item
				n++
			}
		}
		consumer.partitionMessageMap[partition] = list[:n]
	}
}

type TxnSlice []Txn

type Txn struct {
	ts   uint64
	msgs []*Message
}

func (t TxnSlice) Len() int {
	return len(t)
}

func (t TxnSlice) Less(i int, j int) bool {
	return t[i].ts < t[j].ts
}

func (t TxnSlice) Swap(i int, j int) {
	t[i], t[j] = t[j], t[i]
}

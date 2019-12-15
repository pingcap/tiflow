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
	session offsetCommitter
	sink    Sink
	started bool

	cdcResolveTsMap     sync.Map
	partitionMessageMap sync.Map

	tableInfoMap    sync.Map
	tableName2IdMap sync.Map

	lock       sync.Mutex
	metaGroup  *sync.WaitGroup
	cleanGroup *sync.WaitGroup
	cdcCount   int

	persistSig chan uint64
}

type decodedKafkaMessage struct {
	partition int32
	offset    int64
	resolveTs uint64
	message   *Message
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
		client:     consumerGroup,
		topic:      kafkaTopic,
		persistSig: make(chan uint64, 13),
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

	go consumer.tick()

	return nil
}

func (consumer *messageConsumer) Setup(session sarama.ConsumerGroupSession) error {
	consumer.session = session
	return nil
}

func (consumer *messageConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *messageConsumer) TableByID(id int64) (*timodel.TableInfo, bool) {
	if info, ok := consumer.tableInfoMap.Load(id); ok {
		return info.(*timodel.TableInfo), true
	}
	return nil, false
}

func (consumer *messageConsumer) GetTableIDByName(schema, table string) (int64, bool) {
	if id, ok := consumer.tableName2IdMap.Load(FormMapKey(schema, table)); ok {
		return id.(int64), true
	}
	return 0, false
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
		consumer.processResolveRSMsg(partition, offset, msg, committer)
	case MetaType: //cdc is added or deleted
		consumer.processMetaMsg(committer, msg)()
	}
}

func (consumer *messageConsumer) processTxnMsg(partition int32, offset int64, msg *Message) {
	for key, v := range msg.TableInfos {
		consumer.tableInfoMap.Store(v.ID, v)
		consumer.tableName2IdMap.Store(key, v.ID)
	}

	wrapper := &decodedKafkaMessage{message: msg, partition: partition, offset: offset}
	list, _ := consumer.partitionMessageMap.LoadOrStore(wrapper.partition, &messageList{})
	list.(*messageList).add(wrapper)
}
func (consumer *messageConsumer) processResolveRSMsg(partition int32, offset int64, msg *Message, committer offsetCommitter) {
	rsMsg := &decodedKafkaMessage{resolveTs: msg.ResloveTs, partition: partition, offset: offset}
	rsList, _ := consumer.cdcResolveTsMap.LoadOrStore(msg.CdcID, &messageList{})
	rsList.(*messageList).add(rsMsg)

	//add to partition cache too
	partitionMsg := &decodedKafkaMessage{message: msg, partition: partition, offset: offset}
	partitionList, _ := consumer.partitionMessageMap.LoadOrStore(partitionMsg.partition, &messageList{})
	partitionList.(*messageList).add(partitionMsg)

	go func() { consumer.persistSig <- msg.ResloveTs }()
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
			go func() { consumer.persistSig <- msg.ResloveTs }()

			//after this time the cdc node count is changed
			consumer.cdcCount = len(msg.CdcList)
			existsMap := map[string]bool{}
			for _, cdcName := range msg.CdcList {
				existsMap[cdcName] = true
			}
			consumer.cdcResolveTsMap.Range(func(key, value interface{}) bool {
				if !existsMap[key.(string)] {
					//cdc is deleted
					consumer.cdcResolveTsMap.Delete(key)
				}
				return true
			})
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

	for {
		//check if we received all RS from all cdc node
		if consumer.cdcCount > 0 {
			size := 0
			consumer.cdcResolveTsMap.Range(func(key, value interface{}) bool {
				size++
				return true
			})
			if consumer.cdcCount > size {
				return
			}

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
			v, _ := consumer.cdcResolveTsMap.Load(minRsCdcName)
			v.(*messageList).DeleteFirst()
			//delete saved messages
			consumer.deleteSaveKafkaMessage(minRS)
		} else {
			return
		}
	}
}

func (consumer *messageConsumer) calCommitOffset(minRS uint64) map[int32]int64 {
	offsetMap := map[int32]int64{}
	consumer.partitionMessageMap.Range(func(key, value interface{}) bool {
		value.(*messageList).Range(func(i int, msg *decodedKafkaMessage) {
			if msg.message.MsgType == ResolveTsType && msg.message.ResloveTs == minRS {
				offsetMap[key.(int32)] = msg.offset
			}
		})
		return true
	})
	return offsetMap
}

func (consumer *messageConsumer) getTxnMap(minRS uint64) map[uint64][]*Message {
	txnMap := map[uint64][]*Message{}
	consumer.partitionMessageMap.Range(func(key, value interface{}) bool {
		value.(*messageList).Range(
			func(i int, msg *decodedKafkaMessage) {
				if msg.message.MsgType == TxnType {
					if msg.message.Txn.Ts <= minRS {
						txnMessages := txnMap[msg.message.Txn.Ts]
						txnMessages = append(txnMessages, msg.message)
						txnMap[msg.message.Txn.Ts] = txnMessages
					}
				}
			})
		return true
	})
	return txnMap
}

func (consumer *messageConsumer) findMinRs() (uint64, string, bool, map[int32]int64) {
	minRS := uint64(math.MaxUint64)
	offsetMap := map[int32]int64{}
	minRsCdcName := ""
	skip := false
	consumer.cdcResolveTsMap.Range(func(key, value interface{}) bool {
		msg := value.(*messageList)
		if msg.length() <= 0 { //has no rs, we can not calculate the min rs, skip
			skip = true
			return false
		}

		if msg.index(0).resolveTs < minRS {
			minRS = msg.index(0).resolveTs
			minRsCdcName = key.(string)
			offsetMap[msg.index(0).partition] = msg.index(0).offset
		}
		return true
	})
	return minRS, minRsCdcName, skip, offsetMap
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
	consumer.partitionMessageMap.Range(func(key, value interface{}) bool {
		value.(*messageList).delete(minRS)
		return true
	})
}

func (consumer *messageConsumer) tick() {
	for {
		select {
		case rs := <-consumer.persistSig:
			log.Info("start to process resolved rs", zap.Uint64("rs", rs))
			consumer.tryPersistent(consumer.session)
		}
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

type messageList struct {
	lock    sync.RWMutex
	msgList []*decodedKafkaMessage
}

func (l *messageList) add(m *decodedKafkaMessage) {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.msgList = append(l.msgList, m)
}

func (l *messageList) delete(minRS uint64) {
	l.lock.Lock()
	defer l.lock.Unlock()

	n := 0
	for _, item := range l.msgList {
		if (item.message.MsgType == ResolveTsType && item.message.ResloveTs <= minRS) ||
			(item.message.MsgType == TxnType && item.message.Txn.Ts > minRS) {
			l.msgList[n] = item
			n++
		}
	}
	l.msgList = l.msgList[:n]
}

func (l *messageList) length() int {
	l.lock.RLock()
	defer l.lock.RUnlock()

	return len(l.msgList)
}

func (l *messageList) index(index int) *decodedKafkaMessage {
	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.msgList[index]
}

func (l *messageList) DeleteFirst() {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.msgList = l.msgList[1:]
}

func (l *messageList) Range(f func(i int, message *decodedKafkaMessage)) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	for index, msg := range l.msgList {
		f(index, msg)
	}
}

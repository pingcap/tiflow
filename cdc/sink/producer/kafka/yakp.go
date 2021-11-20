// Copyright 2021 PingCAP, Inc.
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

package kafka

type yakProducer struct {
	partitionNum int
}

//func (k *kafkaSaramaProducer) AsyncSendMessage(ctx context.Context, message *codec.MQMessage, partition int32) error {
//	k.clientLock.RLock()
//	defer k.clientLock.RUnlock()
//
//	// Checks whether the producer is closing.
//	// The atomic flag must be checked under `clientLock.RLock()`
//	if atomic.LoadInt32(&k.closing) == kafkaProducerClosing {
//		return nil
//	}
//
//	msg := &sarama.ProducerMessage{
//		Topic:     k.topic,
//		Key:       sarama.ByteEncoder(message.Key),
//		Value:     sarama.ByteEncoder(message.Value),
//		Partition: partition,
//	}
//	msg.Metadata = atomic.AddUint64(&k.partitionOffset[partition].sent, 1)
//
//	failpoint.Inject("KafkaSinkAsyncSendError", func() {
//		// simulate sending message to input channel successfully but flushing
//		// message to Kafka meets error
//		log.Info("failpoint error injected")
//		k.failpointCh <- errors.New("kafka sink injected error")
//		failpoint.Return(nil)
//	})
//
//	failpoint.Inject("SinkFlushDMLPanic", func() {
//		time.Sleep(time.Second)
//		log.Panic("SinkFlushDMLPanic")
//	})
//
//	select {
//	case <-ctx.Done():
//		return ctx.Err()
//	case <-k.closeCh:
//		return nil
//	case k.asyncClient.Input() <- msg:
//	}
//	return nil
//}
//
//func (k *kafkaSaramaProducer) SyncBroadcastMessage(ctx context.Context, message *codec.MQMessage) error {
//	k.clientLock.RLock()
//	defer k.clientLock.RUnlock()
//	msgs := make([]*sarama.ProducerMessage, k.partitionNum)
//	for i := 0; i < int(k.partitionNum); i++ {
//		msgs[i] = &sarama.ProducerMessage{
//			Topic:     k.topic,
//			Key:       sarama.ByteEncoder(message.Key),
//			Value:     sarama.ByteEncoder(message.Value),
//			Partition: int32(i),
//		}
//	}
//	select {
//	case <-ctx.Done():
//		return ctx.Err()
//	case <-k.closeCh:
//		return nil
//	default:
//		err := k.syncClient.SendMessages(msgs)
//		return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
//	}
//}
//
//func (k *kafkaSaramaProducer) Flush(ctx context.Context) error {
//	targetOffsets := make([]uint64, k.partitionNum)
//	for i := 0; i < len(k.partitionOffset); i++ {
//		targetOffsets[i] = atomic.LoadUint64(&k.partitionOffset[i].sent)
//	}
//
//	noEventsToFLush := true
//	for i, target := range targetOffsets {
//		if target > atomic.LoadUint64(&k.partitionOffset[i].flushed) {
//			noEventsToFLush = false
//			break
//		}
//	}
//	if noEventsToFLush {
//		// no events to flush
//		return nil
//	}
//
//	// checkAllPartitionFlushed checks whether data in each partition is flushed
//	checkAllPartitionFlushed := func() bool {
//		for i, target := range targetOffsets {
//			if target > atomic.LoadUint64(&k.partitionOffset[i].flushed) {
//				return false
//			}
//		}
//		return true
//	}
//
//flushLoop:
//	for {
//		select {
//		case <-ctx.Done():
//			return ctx.Err()
//		case <-k.closeCh:
//			if checkAllPartitionFlushed() {
//				return nil
//			}
//			return cerror.ErrKafkaFlushUnfinished.GenWithStackByArgs()
//		case <-k.flushedReceiver.C:
//			if !checkAllPartitionFlushed() {
//				continue flushLoop
//			}
//			return nil
//		}
//	}
//}
//
//func (k *kafkaSaramaProducer) GetPartitionNum() int32 {
//	return k.partitionNum
//}

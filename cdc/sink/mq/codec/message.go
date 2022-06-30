// Copyright 2022 PingCAP, Inc.
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

package codec

import (
	"encoding/binary"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/tikv/client-go/v2/oracle"
)

// MQMessage represents an MQ message to the mqSink
type MQMessage struct {
	Key       []byte
	Value     []byte
	Ts        uint64            // reserved for possible output sorting
	Schema    *string           // schema
	Table     *string           // table
	Type      model.MessageType // type
	Protocol  config.Protocol   // protocol
	rowsCount int               // rows in one MQ Message
}

// maximumRecordOverhead is used to calculate ProducerMessage's byteSize by sarama kafka client.
// reference: https://github.com/Shopify/sarama/blob/66521126c71c522c15a36663ae9cddc2b024c799/async_producer.go#L233
// for TiCDC, minimum supported kafka version is `0.11.0.2`, which will be treated as `version = 2` by sarama producer.
const maximumRecordOverhead = 5*binary.MaxVarintLen32 + binary.MaxVarintLen64 + 1

// Length returns the expected size of the Kafka message
// We didn't append any `Headers` when send the message, so ignore the calculations related to it.
// If `ProducerMessage` Headers fields used, this method should also adjust.
func (m *MQMessage) Length() int {
	return len(m.Key) + len(m.Value) + maximumRecordOverhead
}

// PhysicalTime returns physical time part of Ts in time.Time
func (m *MQMessage) PhysicalTime() time.Time {
	return oracle.GetTimeFromTS(m.Ts)
}

// GetRowsCount returns the number of rows batched in one MQMessage
func (m *MQMessage) GetRowsCount() int {
	return m.rowsCount
}

// SetRowsCount set the number of rows
func (m *MQMessage) SetRowsCount(cnt int) {
	m.rowsCount = cnt
}

// IncRowsCount increase the number of rows
func (m *MQMessage) IncRowsCount() {
	m.rowsCount++
}

func newDDLMQMessage(proto config.Protocol, key, value []byte, event *model.DDLEvent) *MQMessage {
	return NewMQMessage(
		proto,
		key,
		value,
		event.CommitTs,
		model.MessageTypeDDL,
		&event.TableInfo.Schema,
		&event.TableInfo.Table,
	)
}

func newResolvedMQMessage(proto config.Protocol, key, value []byte, ts uint64) *MQMessage {
	return NewMQMessage(proto, key, value, ts, model.MessageTypeResolved, nil, nil)
}

// NewMQMessage should be used when creating a MQMessage struct.
// It copies the input byte slices to avoid any surprises in asynchronous MQ writes.
func NewMQMessage(
	proto config.Protocol,
	key []byte,
	value []byte,
	ts uint64,
	ty model.MessageType,
	schema, table *string,
) *MQMessage {
	ret := &MQMessage{
		Key:       nil,
		Value:     nil,
		Ts:        ts,
		Schema:    schema,
		Table:     table,
		Type:      ty,
		Protocol:  proto,
		rowsCount: 0,
	}

	if key != nil {
		ret.Key = make([]byte, len(key))
		copy(ret.Key, key)
	}

	if value != nil {
		ret.Value = make([]byte, len(value))
		copy(ret.Value, value)
	}

	return ret
}

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

package common

import (
	"encoding/binary"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/tikv/client-go/v2/oracle"
)

// MaxRecordOverhead is used to calculate message size by sarama kafka client.
// reference: https://github.com/Shopify/sarama/blob/
// 66521126c71c522c15a36663ae9cddc2b024c799/async_producer.go#L233
// For TiCDC, minimum supported kafka version is `0.11.0.2`,
// which will be treated as `version = 2` by sarama producer.
const MaxRecordOverhead = 5*binary.MaxVarintLen32 + binary.MaxVarintLen64 + 1

// Message represents an message to the sink
type Message struct {
	Key       []byte
	Value     []byte
	Ts        uint64            // reserved for possible output sorting
	Schema    *string           // schema
	Table     *string           // table
	Type      model.MessageType // type
	Protocol  config.Protocol   // protocol
	rowsCount int               // rows in one Message
	Callback  func()            // Callback function will be called when the message is sent to the sink.

	// ClaimCheckFileName is set if the message should be sent to the claim check storage.
	ClaimCheckFileName string

	Event *model.RowChangedEvent
}

// Length returns the expected size of the Kafka message
// We didn't append any `Headers` when send the message, so ignore the calculations related to it.
// If `ProducerMessage` Headers fields used, this method should also adjust.
func (m *Message) Length() int {
	return len(m.Key) + len(m.Value) + MaxRecordOverhead
}

// PhysicalTime returns physical time part of Ts in time.Time
func (m *Message) PhysicalTime() time.Time {
	return oracle.GetTimeFromTS(m.Ts)
}

// GetRowsCount returns the number of rows batched in one Message
func (m *Message) GetRowsCount() int {
	return m.rowsCount
}

// SetRowsCount set the number of rows
func (m *Message) SetRowsCount(cnt int) {
	m.rowsCount = cnt
}

// IncRowsCount increase the number of rows
func (m *Message) IncRowsCount() {
	m.rowsCount++
}

// NewDDLMsg creates a DDL message.
func NewDDLMsg(proto config.Protocol, key, value []byte, event *model.DDLEvent) *Message {
	return NewMsg(
		proto,
		key,
		value,
		event.CommitTs,
		model.MessageTypeDDL,
		&event.TableInfo.TableName.Schema,
		&event.TableInfo.TableName.Table,
	)
}

// NewResolvedMsg creates a resolved ts message.
func NewResolvedMsg(proto config.Protocol, key, value []byte, ts uint64) *Message {
	return NewMsg(proto, key, value, ts, model.MessageTypeResolved, nil, nil)
}

// NewMsg should be used when creating a Message struct.
// It copies the input byte slices to avoid any surprises in asynchronous MQ writes.
func NewMsg(
	proto config.Protocol,
	key []byte,
	value []byte,
	ts uint64,
	ty model.MessageType,
	schema, table *string,
) *Message {
	ret := &Message{
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

// ClaimCheckMessage is the message sent to the claim-check external storage.
type ClaimCheckMessage struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// UnmarshalClaimCheckMessage unmarshal bytes to ClaimCheckMessage.
func UnmarshalClaimCheckMessage(data []byte) (*ClaimCheckMessage, error) {
	var m ClaimCheckMessage
	err := json.Unmarshal(data, &m)
	return &m, err
}

// NewClaimCheckFileName return file name for sent the message to claim check storage.
func NewClaimCheckFileName(e *model.RowChangedEvent) string {
	prefix := []string{e.Table.Schema, e.Table.Table, strconv.FormatUint(e.CommitTs, 10)}
	elements := append(prefix, e.GetHandleKeyColumnValues()...)
	fileName := strings.Join(elements, "-")
	fileName += ".json"
	// the maximum length of the S3 object key is 1024 bytes,
	// ref https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
	if len(fileName) > 1024 {
		// randomly generated uuid has 122 bits, it should be within the length limit with the prefix.
		prefix = append(prefix, uuid.New().String())
		fileName = strings.Join(prefix, "-")
	}
	return fileName
}

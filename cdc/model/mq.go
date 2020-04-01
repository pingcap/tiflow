package model

import (
	"bytes"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/util/codec"
)

// MqMessageType is the type of message
type MqMessageType int

const (
	// MqMessageTypeUnknow is unknown type of message key
	MqMessageTypeUnknow MqMessageType = iota
	// MqMessageTypeRow is row type of message key
	MqMessageTypeRow
	// MqMessageTypeDDL is ddl type of message key
	MqMessageTypeDDL
	// MqMessageTypeResolved is resolved type of message key
	MqMessageTypeResolved
)

// MqMessageKey represents the message key
type MqMessageKey struct {
	Ts     uint64        `json:"ts"`
	Schema string        `json:"scm,omitempty"`
	Table  string        `json:"tbl,omitempty"`
	Type   MqMessageType `json:"t"`
}

// Encode encodes the message to the json bytes
func (m *MqMessageKey) Encode() ([]byte, error) {
	return json.Marshal(m)

}

// Decode decodes the message from json bytes
func (m *MqMessageKey) Decode(data []byte) error {
	return json.Unmarshal(data, m)
}

// MqMessageRow represents the row message value
type MqMessageRow struct {
	Update map[string]*Column `json:"u,omitempty"`
	Delete map[string]*Column `json:"d,omitempty"`
}

// Encode encodes the message to the json bytes
func (m *MqMessageRow) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// Decode decodes the message from json bytes
func (m *MqMessageRow) Decode(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		return errors.Trace(err)
	}
	for _, column := range m.Update {
		column.formatVal()
	}
	for _, column := range m.Delete {
		column.formatVal()
	}
	return nil
}

// MqMessageDDL represents the DDL message value
type MqMessageDDL struct {
	Query string           `json:"q"`
	Type  model.ActionType `json:"t"`
}

// Encode encodes the message to the json bytes
func (m *MqMessageDDL) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// Decode decodes the message from json bytes
func (m *MqMessageDDL) Decode(data []byte) error {
	return json.Unmarshal(data, m)
}

// NewResolvedMessage creates a new message key which of type is Resolved
func NewResolvedMessage(ts uint64) *MqMessageKey {
	return &MqMessageKey{
		Ts:   ts,
		Type: MqMessageTypeResolved,
	}
}

type BatchEncoder struct {
	keyBuf   *bytes.Buffer
	valueBuf *bytes.Buffer
}

func (batch *BatchEncoder) Append(key []byte, value []byte) {
	batch.keyBuf.Write(codec.EncodeInt([]byte{}, int64(len(key))))
	batch.keyBuf.Write(key)

	batch.valueBuf.Write(codec.EncodeInt([]byte{}, int64(len(value))))
	batch.valueBuf.Write(value)
}

func (batch *BatchEncoder) Marshal() ([]byte, []byte) {
	return batch.keyBuf.Bytes(), batch.valueBuf.Bytes()
}

func (batch *BatchEncoder) Len() int {
	return batch.keyBuf.Len() + batch.valueBuf.Len()
}

func NewBatchEncoder() *BatchEncoder {
	return &BatchEncoder{
		keyBuf:   &bytes.Buffer{},
		valueBuf: &bytes.Buffer{},
	}
}

type BatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
}

func (batch *BatchDecoder) Set(key []byte, value []byte) {
	batch.keyBytes = key
	batch.valueBytes = value
}

func (batch *BatchDecoder) HasNext() bool {
	return len(batch.keyBytes) > 0 && len(batch.valueBytes) > 0
}

func (batch *BatchDecoder) Next() ([]byte, []byte, error) {
	keyLeft, keyLen, err := codec.DecodeInt(batch.keyBytes)
	if err != nil {
		return nil, nil, err
	}
	key := keyLeft[0:keyLen]
	batch.keyBytes = keyLeft[keyLen:]

	valueLeft, valueLen, err := codec.DecodeInt(batch.valueBytes)
	if err != nil {
		return nil, nil, err
	}
	value := valueLeft[0:valueLen]
	batch.valueBytes = valueLeft[valueLen:]
	return key, value, nil
}

func NewBatchDecoder() *BatchDecoder {
	return &BatchDecoder{}
}

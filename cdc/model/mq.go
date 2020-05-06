package model

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
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

const (
	// BatchVersion1 represents the version of batch format
	BatchVersion1 uint64 = 1
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

// BatchDecoder decodes the byte of a batch into the original messages.
type BatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
}

// Set sets the byte of the decoded batch.
func (batch *BatchDecoder) Set(key []byte, value []byte) error {
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != BatchVersion1 {
		return errors.New("unexpected key format version")
	}
	batch.keyBytes = key
	batch.valueBytes = value
	return nil
}

// HasNext returns whether there is a next message in the batch.
func (batch *BatchDecoder) HasNext() bool {
	return len(batch.keyBytes) > 0 && len(batch.valueBytes) > 0
}

// Next returns the next message. It must be used when HasNext is true.
func (batch *BatchDecoder) Next() ([]byte, []byte, bool) {
	if !batch.HasNext() {
		return nil, nil, false
	}
	keyLen := binary.BigEndian.Uint64(batch.keyBytes[:8])
	valueLen := binary.BigEndian.Uint64(batch.valueBytes[:8])
	key := batch.keyBytes[8 : keyLen+8]
	value := batch.valueBytes[8 : valueLen+8]
	batch.keyBytes = batch.keyBytes[keyLen+8:]
	batch.valueBytes = batch.valueBytes[valueLen+8:]
	return key, value, true
}

// NewBatchDecoder creates a new BatchDecoder.
func NewBatchDecoder() *BatchDecoder {
	return &BatchDecoder{}
}

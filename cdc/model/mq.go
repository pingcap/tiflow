package model

import (
	"bytes"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
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
	BatchVersion1 int64 = 1
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

// BatchEncoder encodes messages into a batch
type BatchEncoder struct {
	keyBuf   *bytes.Buffer
	valueBuf *bytes.Buffer
}

// Append adds a message to the batch
func (batch *BatchEncoder) Append(key []byte, value []byte) {
	batch.keyBuf.Write(codec.EncodeInt([]byte{}, int64(len(key))))
	batch.keyBuf.Write(key)

	batch.valueBuf.Write(codec.EncodeInt([]byte{}, int64(len(value))))
	batch.valueBuf.Write(value)

	log.Debug("append msg to batch", zap.Int("batchSize", batch.Len()), zap.Int("keySize", len(key)), zap.Int("valueSize", len(value)))
}

// Read reads the current batch from the buffer.
func (batch *BatchEncoder) Read() (keyByte []byte, valueByte []byte) {
	keyByte = make([]byte, batch.keyBuf.Len())
	_, _ = batch.keyBuf.Read(keyByte)

	valueByte = make([]byte, batch.valueBuf.Len())
	_, _ = batch.valueBuf.Read(valueByte)
	return
}

// Len returns the size of the current batch.
func (batch *BatchEncoder) Len() int {
	return batch.keyBuf.Len() + batch.valueBuf.Len() - 8
}

// Reset resets the buffer to be empty.
func (batch *BatchEncoder) Reset() {
	batch.keyBuf.Reset()
	batch.keyBuf.Write(codec.EncodeInt([]byte{}, BatchVersion1))
	batch.valueBuf.Reset()
}

// NewBatchEncoder creates a new BatchEncoder.
func NewBatchEncoder() *BatchEncoder {
	batch := &BatchEncoder{
		keyBuf:   &bytes.Buffer{},
		valueBuf: &bytes.Buffer{},
	}
	batch.Reset()
	return batch
}

// BatchDecoder decodes the byte of a batch into the original messages.
type BatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
}

// Set sets the byte of the decoded batch.
func (batch *BatchDecoder) Set(key []byte, value []byte) error {
	keyLeft, keyVersion, err := codec.DecodeInt(key)
	if err != nil {
		return err
	}
	if keyVersion != BatchVersion1 {
		return errors.New("unexpected key format version")
	}
	batch.keyBytes = keyLeft
	batch.valueBytes = value
	return nil
}

// HasNext returns whether there is a next message in the batch.
func (batch *BatchDecoder) HasNext() bool {
	return len(batch.keyBytes) > 0 && len(batch.valueBytes) > 0
}

// Next returns the next message. It must be used when HasNext is true.
func (batch *BatchDecoder) Next() ([]byte, []byte, error) {
	keyLeft, keyLen, err := codec.DecodeInt(batch.keyBytes)
	if err != nil {
		return nil, nil, err
	}
	valueLeft, valueLen, err := codec.DecodeInt(batch.valueBytes)
	if err != nil {
		return nil, nil, err
	}

	log.Debug("decode msg", zap.Int64("keySize", keyLen), zap.Int64("valueSize", valueLen), zap.Int("keyLeft", len(keyLeft)), zap.Int("valueLeft", len(valueLeft)))

	key := keyLeft[0:keyLen]
	batch.keyBytes = keyLeft[keyLen:]

	value := valueLeft[0:valueLen]
	batch.valueBytes = valueLeft[valueLen:]
	return key, value, nil
}

// NewBatchDecoder creates a new BatchDecoder.
func NewBatchDecoder() *BatchDecoder {
	return &BatchDecoder{}
}

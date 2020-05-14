package codec

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"log"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

const (
	// BatchVersion1 represents the version of batch format
	BatchVersion1 uint64 = 1
)

type column = model.Column

func formatColumnVal(c *column) {
	switch c.Type {
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob,
		mysql.TypeLongBlob, mysql.TypeBlob:
		if s, ok := c.Value.(string); ok {
			var err error
			c.Value, err = base64.StdEncoding.DecodeString(s)
			if err != nil {
				log.Fatal("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		}
	case mysql.TypeBit:
		if s, ok := c.Value.(json.Number); ok {
			intNum, err := s.Int64()
			if err != nil {
				log.Fatal("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
			c.Value = uint64(intNum)
		}
	}
}

type mqMessageKey struct {
	// TODO: should we rename it to CRTs
	Ts     uint64              `json:"ts"`
	Schema string              `json:"scm,omitempty"`
	Table  string              `json:"tbl,omitempty"`
	Type   model.MqMessageType `json:"t"`
}

func (m *mqMessageKey) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *mqMessageKey) Decode(data []byte) error {
	return json.Unmarshal(data, m)
}

type mqMessageRow struct {
	Update map[string]*column `json:"u,omitempty"`
	Delete map[string]*column `json:"d,omitempty"`
}

func (m *mqMessageRow) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *mqMessageRow) Decode(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		return errors.Trace(err)
	}
	for _, column := range m.Update {
		formatColumnVal(column)
	}
	for _, column := range m.Delete {
		formatColumnVal(column)
	}
	return nil
}

type mqMessageDDL struct {
	Query string             `json:"q"`
	Type  timodel.ActionType `json:"t"`
}

func (m *mqMessageDDL) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *mqMessageDDL) Decode(data []byte) error {
	return json.Unmarshal(data, m)
}

func newResolvedMessage(ts uint64) *mqMessageKey {
	return &mqMessageKey{
		Ts:   ts,
		Type: model.MqMessageTypeResolved,
	}
}

func rowEventToMqMessage(e *model.RowChangedEvent) (*mqMessageKey, *mqMessageRow) {
	key := &mqMessageKey{
		Ts:     e.CommitTs,
		Schema: e.Table.Schema,
		Table:  e.Table.Table,
		Type:   model.MqMessageTypeRow,
	}
	value := &mqMessageRow{}
	if e.Delete {
		value.Delete = e.Columns
	} else {
		value.Update = e.Columns
	}
	return key, value
}

func mqMessageToRowEvent(key *mqMessageKey, value *mqMessageRow) *model.RowChangedEvent {
	e := new(model.RowChangedEvent)
	e.CommitTs = key.Ts
	e.Table = &model.TableName{
		Schema: key.Schema,
		Table:  key.Table,
	}

	if len(value.Delete) != 0 {
		e.Delete = true
		e.Columns = value.Delete
	} else {
		e.Delete = false
		e.Columns = value.Update
	}
	return e
}

func ddlEventtoMqMessage(e *model.DDLEvent) (*mqMessageKey, *mqMessageDDL) {
	key := &mqMessageKey{
		Ts:     e.Ts,
		Schema: e.Schema,
		Table:  e.Table,
		Type:   model.MqMessageTypeDDL,
	}
	value := &mqMessageDDL{
		Query: e.Query,
		Type:  e.Type,
	}
	return key, value
}

func mqMessageToDDLEvent(key *mqMessageKey, value *mqMessageDDL) *model.DDLEvent {
	e := new(model.DDLEvent)
	e.Ts = key.Ts
	e.Table = key.Table
	e.Schema = key.Schema
	e.Type = value.Type
	e.Query = value.Query
	return e
}

// JSONEventBatchEncoder encodes the events into the byte of a batch into.
type JSONEventBatchEncoder struct {
	keyBuf   *bytes.Buffer
	valueBuf *bytes.Buffer
}

// AppendResolvedEvent implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) AppendResolvedEvent(ts uint64) error {
	keyMsg := newResolvedMessage(ts)
	key, err := keyMsg.Encode()
	if err != nil {
		return errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], 0)

	d.keyBuf.Write(keyLenByte[:])
	d.keyBuf.Write(key)

	d.valueBuf.Write(valueLenByte[:])
	return nil
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) error {
	keyMsg, valueMsg := rowEventToMqMessage(e)
	key, err := keyMsg.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	value, err := valueMsg.Encode()
	if err != nil {
		return errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	d.keyBuf.Write(keyLenByte[:])
	d.keyBuf.Write(key)

	d.valueBuf.Write(valueLenByte[:])
	d.valueBuf.Write(value)
	return nil
}

// AppendDDLEvent implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) AppendDDLEvent(e *model.DDLEvent) error {
	keyMsg, valueMsg := ddlEventtoMqMessage(e)
	key, err := keyMsg.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	value, err := valueMsg.Encode()
	if err != nil {
		return errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	d.keyBuf.Write(keyLenByte[:])
	d.keyBuf.Write(key)

	d.valueBuf.Write(valueLenByte[:])
	d.valueBuf.Write(value)
	return nil
}

// Build implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) Build() (key []byte, value []byte) {
	return d.keyBuf.Bytes(), d.valueBuf.Bytes()
}

// Size implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) Size() int {
	return d.keyBuf.Len() + d.valueBuf.Len()
}

// NewJSONEventBatchEncoder creates a new JSONEventBatchEncoder.
func NewJSONEventBatchEncoder() EventBatchEncoder {
	batch := &JSONEventBatchEncoder{
		keyBuf:   &bytes.Buffer{},
		valueBuf: &bytes.Buffer{},
	}
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	batch.keyBuf.Write(versionByte[:])
	return batch
}

// JSONEventBatchDecoder decodes the byte of a batch into the original messages.
type JSONEventBatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
	nextKey    *mqMessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *JSONEventBatchDecoder) HasNext() (model.MqMessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return 0, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeResolved {
		return 0, errors.NotFoundf("not found resolved event message")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	b.valueBytes = b.valueBytes[valueLen+8:]
	resolvedTs := b.nextKey.Ts
	b.nextKey = nil
	return resolvedTs, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeRow {
		return nil, errors.NotFoundf("not found row event message")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	b.valueBytes = b.valueBytes[valueLen+8:]
	rowMsg := new(mqMessageRow)
	if err := rowMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	rowEvent := mqMessageToRowEvent(b.nextKey, rowMsg)
	b.nextKey = nil
	return rowEvent, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeDDL {
		return nil, errors.NotFoundf("not found ddl event message")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	b.valueBytes = b.valueBytes[valueLen+8:]
	ddlMsg := new(mqMessageDDL)
	if err := ddlMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent := mqMessageToDDLEvent(b.nextKey, ddlMsg)
	b.nextKey = nil
	return ddlEvent, nil
}

func (b *JSONEventBatchDecoder) hasNext() bool {
	return len(b.keyBytes) > 0 && len(b.valueBytes) > 0
}

func (b *JSONEventBatchDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(mqMessageKey)
	err := msgKey.Decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.nextKeyLen = keyLen
	return nil
}

// NewJSONEventBatchDecoder creates a new JSONEventBatchDecoder.
func NewJSONEventBatchDecoder(key []byte, value []byte) (EventBatchDecoder, error) {
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != BatchVersion1 {
		return nil, errors.New("unexpected key format version")
	}
	return &JSONEventBatchDecoder{
		keyBytes:   key,
		valueBytes: value,
	}, nil
}

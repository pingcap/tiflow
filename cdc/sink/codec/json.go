// Copyright 2020 PingCAP, Inc.
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
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/cdc/model"
)

const (
	// BatchVersion1 represents the version of batch format
	BatchVersion1 uint64 = 1
)

type column struct {
	Type byte `json:"t"`

	// WhereHandle is deprecation
	// WhereHandle is replaced by HandleKey in Flag
	WhereHandle *bool                `json:"h,omitempty"`
	Flag        model.ColumnFlagType `json:"f"`
	Value       interface{}          `json:"v"`
}

func (c *column) FromSinkColumn(col *model.Column) {
	c.Type = col.Type
	c.Flag = col.Flag
	if c.Flag.IsHandleKey() {
		whereHandle := true
		c.WhereHandle = &whereHandle
	}
	if col.Value == nil {
		c.Value = nil
		return
	}
	switch col.Type {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		str := string(col.Value.([]byte))
		if c.Flag.IsBinary() {
			str = strconv.Quote(str)
			str = str[1 : len(str)-1]
		}
		c.Value = str
	default:
		c.Value = col.Value
	}
}

func (c *column) ToSinkColumn(name string) *model.Column {
	col := new(model.Column)
	col.Type = c.Type
	col.Flag = c.Flag
	col.Name = name
	col.Value = c.Value
	if c.Value == nil {
		return col
	}
	switch col.Type {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		str := col.Value.(string)
		var err error
		if c.Flag.IsBinary() {
			str, err = strconv.Unquote("\"" + str + "\"")
			if err != nil {
				log.Fatal("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		}
		col.Value = []byte(str)
	default:
		col.Value = c.Value
	}
	return col
}

func formatColumnVal(c column) column {
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
	return c
}

type mqMessageKey struct {
	// TODO: should we rename it to CRTs
	Ts        uint64              `json:"ts"`
	Schema    string              `json:"scm,omitempty"`
	Table     string              `json:"tbl,omitempty"`
	RowID     int64               `json:"rid,omitempty"`
	Partition *int64              `json:"ptn,omitempty"`
	Type      model.MqMessageType `json:"t"`
}

func (m *mqMessageKey) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *mqMessageKey) Decode(data []byte) error {
	return cerror.WrapError(cerror.ErrUnmarshalFailed, json.Unmarshal(data, m))
}

type mqMessageRow struct {
	Update     map[string]column `json:"u,omitempty"`
	PreColumns map[string]column `json:"p,omitempty"`
	Delete     map[string]column `json:"d,omitempty"`
}

func (m *mqMessageRow) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *mqMessageRow) Decode(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		return cerror.WrapError(cerror.ErrUnmarshalFailed, err)
	}
	for colName, column := range m.Update {
		m.Update[colName] = formatColumnVal(column)
	}
	for colName, column := range m.Delete {
		m.Delete[colName] = formatColumnVal(column)
	}
	for colName, column := range m.PreColumns {
		m.PreColumns[colName] = formatColumnVal(column)
	}
	return nil
}

type mqMessageDDL struct {
	Query string             `json:"q"`
	Type  timodel.ActionType `json:"t"`
}

func (m *mqMessageDDL) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *mqMessageDDL) Decode(data []byte) error {
	return cerror.WrapError(cerror.ErrUnmarshalFailed, json.Unmarshal(data, m))
}

func newResolvedMessage(ts uint64) *mqMessageKey {
	return &mqMessageKey{
		Ts:   ts,
		Type: model.MqMessageTypeResolved,
	}
}

func rowEventToMqMessage(e *model.RowChangedEvent) (*mqMessageKey, *mqMessageRow) {
	var partition *int64
	if e.Table.IsPartition {
		partition = &e.Table.TableID
	}
	key := &mqMessageKey{
		Ts:        e.CommitTs,
		Schema:    e.Table.Schema,
		Table:     e.Table.Table,
		RowID:     e.RowID,
		Partition: partition,
		Type:      model.MqMessageTypeRow,
	}
	value := &mqMessageRow{}
	if e.IsDelete() {
		value.Delete = sinkColumns2JsonColumns(e.PreColumns)
	} else {
		value.Update = sinkColumns2JsonColumns(e.Columns)
		value.PreColumns = sinkColumns2JsonColumns(e.PreColumns)
	}
	return key, value
}

func sinkColumns2JsonColumns(cols []*model.Column) map[string]column {
	jsonCols := make(map[string]column, len(cols))
	for _, col := range cols {
		if col == nil {
			continue
		}
		c := column{}
		c.FromSinkColumn(col)
		jsonCols[col.Name] = c
	}
	if len(jsonCols) == 0 {
		return nil
	}
	return jsonCols
}

func jsonColumns2SinkColumns(cols map[string]column) []*model.Column {
	sinkCols := make([]*model.Column, 0, len(cols))
	for name, col := range cols {
		c := col.ToSinkColumn(name)
		sinkCols = append(sinkCols, c)
	}
	if len(sinkCols) == 0 {
		return nil
	}
	sort.Slice(sinkCols, func(i, j int) bool {
		return strings.Compare(sinkCols[i].Name, sinkCols[j].Name) > 0
	})
	return sinkCols
}

func mqMessageToRowEvent(key *mqMessageKey, value *mqMessageRow) *model.RowChangedEvent {
	e := new(model.RowChangedEvent)
	// TODO: we lost the startTs from kafka message
	// startTs-based txn filter is out of work
	e.CommitTs = key.Ts
	e.Table = &model.TableName{
		Schema: key.Schema,
		Table:  key.Table,
	}
	// TODO: we lost the tableID from kafka message
	if key.Partition != nil {
		e.Table.TableID = *key.Partition
		e.Table.IsPartition = true
	}

	if len(value.Delete) != 0 {
		e.PreColumns = jsonColumns2SinkColumns(value.Delete)
	} else {
		e.Columns = jsonColumns2SinkColumns(value.Update)
		e.PreColumns = jsonColumns2SinkColumns(value.PreColumns)
	}
	return e
}

func ddlEventtoMqMessage(e *model.DDLEvent) (*mqMessageKey, *mqMessageDDL) {
	key := &mqMessageKey{
		Ts:     e.CommitTs,
		Schema: e.TableInfo.Schema,
		Table:  e.TableInfo.Table,
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
	e.TableInfo = new(model.SimpleTableInfo)
	// TODO: we lost the startTs from kafka message
	// startTs-based txn filter is out of work
	e.CommitTs = key.Ts
	e.TableInfo.Table = key.Table
	e.TableInfo.Schema = key.Schema
	e.Type = value.Type
	e.Query = value.Query
	return e
}

// JSONEventBatchEncoder encodes the events into the byte of a batch into.
type JSONEventBatchEncoder struct {
	keyBuf            *bytes.Buffer
	valueBuf          *bytes.Buffer
	supportMixedBuild bool // TODO decouple this out
}

// SetMixedBuildSupport is used by CDC Log
func (d *JSONEventBatchEncoder) SetMixedBuildSupport(enabled bool) {
	d.supportMixedBuild = enabled
}

// AppendResolvedEvent is no-op
func (d *JSONEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	return EncoderNoOperation, nil
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	keyMsg := newResolvedMessage(ts)
	key, err := keyMsg.Encode()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], 0)

	if d.supportMixedBuild {
		d.keyBuf.Write(keyLenByte[:])
		d.keyBuf.Write(key)
		d.valueBuf.Write(valueLenByte[:])
		return nil, nil
	}

	keyBuf := new(bytes.Buffer)
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	keyBuf.Write(versionByte[:])
	keyBuf.Write(keyLenByte[:])
	keyBuf.Write(key)

	valueBuf := new(bytes.Buffer)
	valueBuf.Write(valueLenByte[:])

	ret := NewMQMessage(keyBuf.Bytes(), valueBuf.Bytes(), ts)
	return ret, nil
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	keyMsg, valueMsg := rowEventToMqMessage(e)
	key, err := keyMsg.Encode()
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}
	value, err := valueMsg.Encode()
	if err != nil {
		return EncoderNoOperation, errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	d.keyBuf.Write(keyLenByte[:])
	d.keyBuf.Write(key)

	d.valueBuf.Write(valueLenByte[:])
	d.valueBuf.Write(value)
	return EncoderNoOperation, nil
}

// EncodeDDLEvent implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error) {
	keyMsg, valueMsg := ddlEventtoMqMessage(e)
	key, err := keyMsg.Encode()
	if err != nil {
		return nil, errors.Trace(err)
	}
	value, err := valueMsg.Encode()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	if d.supportMixedBuild {
		d.keyBuf.Write(keyLenByte[:])
		d.keyBuf.Write(key)
		d.valueBuf.Write(valueLenByte[:])
		d.valueBuf.Write(value)
		return nil, nil
	}

	keyBuf := new(bytes.Buffer)
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	keyBuf.Write(versionByte[:])
	keyBuf.Write(keyLenByte[:])
	keyBuf.Write(key)

	valueBuf := new(bytes.Buffer)
	valueBuf.Write(valueLenByte[:])
	valueBuf.Write(value)

	ret := NewMQMessage(keyBuf.Bytes(), valueBuf.Bytes(), e.CommitTs)
	return ret, nil
}

// UpdateResolvedTs implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) UpdateResolvedTs(ts uint64) (EncoderResult, error) {
	// nothing for now
	return EncoderNeedAsyncWrite, nil
}

// Build implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) Build() (mqMessages []*MQMessage) {
	if d.valueBuf.Len() == 0 {
		return nil
	}

	ret := NewMQMessage(d.keyBuf.Bytes(), d.valueBuf.Bytes(), 0)

	if !d.supportMixedBuild {
		d.keyBuf.Reset()
		d.valueBuf.Reset()
		var versionByte [8]byte
		binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
		d.keyBuf.Write(versionByte[:])
	}
	return []*MQMessage{ret}
}

// MixedBuild implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) MixedBuild(withVersion bool) []byte {
	if !d.supportMixedBuild {
		log.Fatal("mixedBuildSupport not enabled!")
		return nil
	}
	keyBytes := d.keyBuf.Bytes()
	valueBytes := d.valueBuf.Bytes()
	mixedBytes := make([]byte, len(keyBytes)+len(valueBytes))

	index := uint64(0)
	keyIndex := uint64(0)
	valueIndex := uint64(0)

	if withVersion {
		// the first 8 bytes is the version, we should copy directly
		// then skip 8 bytes for next round key value parse
		copy(mixedBytes[:8], keyBytes[:8])
		index = uint64(8)    // skip version
		keyIndex = uint64(8) // skip version
	}

	for {
		if keyIndex >= uint64(len(keyBytes)) {
			break
		}
		keyLen := binary.BigEndian.Uint64(keyBytes[keyIndex : keyIndex+8])
		offset := keyLen + 8
		copy(mixedBytes[index:index+offset], keyBytes[keyIndex:keyIndex+offset])
		keyIndex += offset
		index += offset

		valueLen := binary.BigEndian.Uint64(valueBytes[valueIndex : valueIndex+8])
		offset = valueLen + 8
		copy(mixedBytes[index:index+offset], valueBytes[valueIndex:valueIndex+offset])
		valueIndex += offset
		index += offset
	}
	return mixedBytes
}

// Size implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) Size() int {
	return d.keyBuf.Len() + d.valueBuf.Len()
}

// Reset implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) Reset() {
	d.keyBuf.Reset()
	d.valueBuf.Reset()
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

// JSONEventBatchMixedDecoder decodes the byte of a batch into the original messages.
type JSONEventBatchMixedDecoder struct {
	mixedBytes []byte
	nextKey    *mqMessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *JSONEventBatchMixedDecoder) HasNext() (model.MqMessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchMixedDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return 0, err
		}
	}
	b.mixedBytes = b.mixedBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeResolved {
		return 0, cerror.ErrJSONCodecInvalidData.GenWithStack("not found resolved event message")
	}
	valueLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	b.mixedBytes = b.mixedBytes[valueLen+8:]
	resolvedTs := b.nextKey.Ts
	b.nextKey = nil
	return resolvedTs, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchMixedDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.mixedBytes = b.mixedBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeRow {
		return nil, cerror.ErrJSONCodecInvalidData.GenWithStack("not found row event message")
	}
	valueLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	value := b.mixedBytes[8 : valueLen+8]
	b.mixedBytes = b.mixedBytes[valueLen+8:]
	rowMsg := new(mqMessageRow)
	if err := rowMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	rowEvent := mqMessageToRowEvent(b.nextKey, rowMsg)
	b.nextKey = nil
	return rowEvent, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
func (b *JSONEventBatchMixedDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.mixedBytes = b.mixedBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MqMessageTypeDDL {
		return nil, cerror.ErrJSONCodecInvalidData.GenWithStack("not found ddl event message")
	}
	valueLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	value := b.mixedBytes[8 : valueLen+8]
	b.mixedBytes = b.mixedBytes[valueLen+8:]
	ddlMsg := new(mqMessageDDL)
	if err := ddlMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent := mqMessageToDDLEvent(b.nextKey, ddlMsg)
	b.nextKey = nil
	return ddlEvent, nil
}

func (b *JSONEventBatchMixedDecoder) hasNext() bool {
	return len(b.mixedBytes) > 0
}

func (b *JSONEventBatchMixedDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	key := b.mixedBytes[8 : keyLen+8]
	// drop value bytes
	msgKey := new(mqMessageKey)
	err := msgKey.Decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.nextKeyLen = keyLen
	return nil
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
		return 0, cerror.ErrJSONCodecInvalidData.GenWithStack("not found resolved event message")
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
		return nil, cerror.ErrJSONCodecInvalidData.GenWithStack("not found row event message")
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
		return nil, cerror.ErrJSONCodecInvalidData.GenWithStack("not found ddl event message")
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
		return nil, cerror.ErrJSONCodecInvalidData.GenWithStack("unexpected key format version")
	}
	// if only decode one byte slice, we choose MixedDecoder
	if len(key) > 0 && len(value) == 0 {
		return &JSONEventBatchMixedDecoder{
			mixedBytes: key,
		}, nil
	}
	return &JSONEventBatchDecoder{
		keyBytes:   key,
		valueBytes: value,
	}, nil
}

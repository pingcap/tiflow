package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
)

type MaxwellEventBatchEncoder struct {
	keyBuf   *bytes.Buffer
	valueBuf *bytes.Buffer
}

type maxwellMessage struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Type     string                 `json:"type"`
	Ts       uint64                 `json:"ts"`
	Xid      int                    `json:"xid"`
	Xoffset  int                    `json:"xoffset"`
	Position string                 `json:"position"`
	Gtid     string                 `json:"gtid"`
	Data     map[string]interface{} `json:"data"`
	Old      map[string]interface{} `json:"old,omitempty"`
}

func (m *maxwellMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *maxwellMessage) Decode(data []byte) error {
	return json.Unmarshal(data, m)
}

func (m *DdlMaxwellMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// AppendResolvedEvent implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	// For maxwell now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore the event is ignored.
	return EncoderNoOperation, nil
}
func rowEventToMaxwellMessage(e *model.RowChangedEvent) (*mqMessageKey, *maxwellMessage) {
	var partition *int64
	if e.Table.Partition != 0 {
		partition = &e.Table.Partition
	}
	key := &mqMessageKey{
		Ts:        e.CommitTs,
		Schema:    e.Table.Schema,
		Table:     e.Table.Table,
		Partition: partition,
		Type:      model.MqMessageTypeRow,
	}
	value := &maxwellMessage{
		Ts:       e.CommitTs,
		Database: e.Table.Schema,
		Table:    e.Table.Table,
		Type:     "update",
		Xid:      1,
		Xoffset:  1,
		Position: "",
		Gtid:     "",
		Data:     make(map[string]interface{}),
		Old:      make(map[string]interface{}),
	}
	for k, v := range e.Columns {
		value.Data[k] = v.Value
	}
	fmt.Print(e.PreColumns)
	for k, v := range e.PreColumns {
		value.Old[k] = v.Value
	}
	if e.Delete {
		value.Type = "delete"
	} else {
		value.Type = "update"
	}
	return key, value
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error) {
	keyMsg, valueMsg := rowEventToMaxwellMessage(e)
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

type Column struct {
	Type string `json:"type"`
	Name string `json:"name"`
	//Do not mark the unique key temporarily
	Signed       bool   `json:"signed,omitempty"`
	ColumnLength int    `json:"column-length,omitempty"`
	Charset      string `json:"charset,omitempty"`
}

type TableStruct struct {
	Database string    `json:"database"`
	Charset  string    `json:"charset,omitempty"`
	Table    string    `json:"table"`
	Columns  []*Column `json:"columns"`
	//Do not output whether it is a primary key temporarily
	PrimaryKey []string `json:"primary-key"`
}

//maxwell alter table message
//Old for table old schema
//Def for table after ddl schema
type DdlMaxwellMessage struct {
	Type     string      `json:"type"`
	Database string      `json:"database"`
	Table    string      `json:"table"`
	Old      TableStruct `json:"old,omitempty"`
	Def      TableStruct `json:"def,omitempty"`
	Ts       uint64      `json:"ts"`
	Sql      string      `json:"sql"`
	Position string      `json:"position,omitempty"`
}

func ddlEventtoMaxwellMessage(e *model.DDLEvent) (*mqMessageKey, *DdlMaxwellMessage) {
	key := &mqMessageKey{
		Ts:     e.CommitTs,
		Schema: e.TableInfo.Schema,
		Table:  e.TableInfo.Table,
		Type:   model.MqMessageTypeDDL,
	}
	value := &DdlMaxwellMessage{
		Ts:       e.CommitTs,
		Database: e.TableInfo.Schema,
		Type:     e.Type.String(),
		Table:    e.TableInfo.Table,
		Old:      TableStruct{},
		Def:      TableStruct{},
		Sql:      e.Query,
	}
	if e.PreTableInfo != nil {
		value.Old.Database = e.PreTableInfo.Schema
		value.Old.Table = e.PreTableInfo.Table
		for _, v := range e.PreTableInfo.ColumnInfo {
			maxwellcolumntype, _ := columnToMaxwellType(v.Type)
			value.Old.Columns = append(value.Old.Columns, &Column{
				Name: v.Name,
				Type: maxwellcolumntype,
			})
		}
	}

	value.Def.Database = e.TableInfo.Schema
	value.Def.Table = e.TableInfo.Table
	for _, v := range e.TableInfo.ColumnInfo {
		maxwellcolumntype, err := columnToMaxwellType(v.Type)
		if err != nil {
			value.Old.Columns = append(value.Old.Columns, &Column{
				Name: v.Name,
				Type: err.Error(),
			})

		}
		value.Def.Columns = append(value.Def.Columns, &Column{
			Name: v.Name,
			Type: maxwellcolumntype,
		})
	}
	return key, value
}

// AppendDDLEvent implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) AppendDDLEvent(e *model.DDLEvent) (EncoderResult, error) {
	keyMsg, valueMsg := ddlEventtoMaxwellMessage(e)
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
	return EncoderNeedSyncWrite, nil
}

// Build implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) Build() (key []byte, value []byte) {
	return d.keyBuf.Bytes(), d.valueBuf.Bytes()
}

// Size implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) Size() int {
	return d.keyBuf.Len() + d.valueBuf.Len()
}

// NewMaxwellEventBatchEncoder creates a new MaxwellEventBatchEncoder.
func NewMaxwellEventBatchEncoder() EventBatchEncoder {
	batch := &MaxwellEventBatchEncoder{
		keyBuf:   &bytes.Buffer{},
		valueBuf: &bytes.Buffer{},
	}
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	batch.keyBuf.Write(versionByte[:])
	return batch
}

func (b *MaxwellEventBatchDecoder) decodeNextKey() error {
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

func (b *MaxwellEventBatchDecoder) hasNext() bool {
	return len(b.keyBytes) > 0 && len(b.valueBytes) > 0
}

// MaxwellEventBatchDecoder decodes the byte of a batch into the original messages.
type MaxwellEventBatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
	nextKey    *mqMessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *MaxwellEventBatchDecoder) HasNext() (model.MqMessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

func maxwellMessageToRowEvent(key *mqMessageKey, value *maxwellMessage) *model.RowChangedEvent {
	e := new(model.RowChangedEvent)
	e.CommitTs = key.Ts
	e.Table = &model.TableName{
		Schema: key.Schema,
		Table:  key.Table,
	}
	if key.Partition != nil {
		e.Table.Partition = *key.Partition
	}

	if value.Type == "delete" {
		e.Delete = true
		for k, v := range e.Columns {
			v.Value = value.Data[k]
		}
	} else {
		e.Delete = false
		for k, v := range e.Columns {
			v.Value = value.Data[k]
		}
	}
	return e
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *MaxwellEventBatchDecoder) NextResolvedEvent() (uint64, error) {
	// For maxwell now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore the event is ignored.
	return 0, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
func (b *MaxwellEventBatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
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
	rowMsg := new(maxwellMessage)
	if err := rowMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	rowEvent := maxwellMessageToRowEvent(b.nextKey, rowMsg)
	b.nextKey = nil
	return rowEvent, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
func (b *MaxwellEventBatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
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

// NewMaxwellEventBatchDecoder creates a new JSONEventBatchDecoder.
func NewMaxwellEventBatchDecoder(key []byte, value []byte) (EventBatchDecoder, error) {
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != BatchVersion1 {
		return nil, errors.New("unexpected key format version")
	}
	return &MaxwellEventBatchDecoder{
		keyBytes:   key,
		valueBytes: value,
	}, nil
}

//Convert column type code to maxwell column type
func columnToMaxwellType(columnType byte) (string, error) {
	switch columnType {
	case 1:
		return "int", nil
	case 2:
		return "int", nil
	case 9:
		return "int", nil
	case 3:
		return "int", nil
	case 8:
		return "bigint", nil
	case 249:
		return "string", nil
	case 252:
		return "string", nil
	case 250:
		return "string", nil
	case 251:
		return "string", nil
	case 254:
		return "string", nil
	case 15:
		return "string", nil
	case 10:
		return "date", nil
	case 12:
		return "datetime", nil
	case 7:
		return "datetime", nil
	case 11:
		return "time", nil
	case 13:
		return "year", nil
	case 247:
		return "enum", nil
	case 248:
		return "set", nil
	case 16:
		return "bit", nil
	case 245:
		return "json", nil
	case 4:
		return "float", nil
	case 5:
		return "float", nil
	case 246:
		return "decimal", nil
	default:
		return "", errors.Errorf("unsupported column type - %v", columnType)
	}
}

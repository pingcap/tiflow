package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/pingcap/errors"
	model2 "github.com/pingcap/parser/model"
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
	if e.Table.IsPartition {
		partition = &e.Table.TableID
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

	if e.PreColumns == nil {
		value.Type = "insert"
		for _, v := range e.Columns {
			value.Data[v.Name] = v.Value
		}
	} else if e.IsDelete() {
		value.Type = "delete"
		for _, v := range e.PreColumns {
			value.Old[v.Name] = v.Value
		}
	} else {
		value.Type = "update"
		for _, v := range e.Columns {
			value.Data[v.Name] = v.Value
		}
		for _, v := range e.PreColumns {
			value.Old[v.Name] = v.Value
		}
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
		Type:     "table-create",
		Table:    e.TableInfo.Table,
		Old:      TableStruct{},
		Def:      TableStruct{},
		Sql:      e.Query,
	}

	value.Type = ddlToMaxwellType(e.Type)

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

// MixedBuild implements the EventBatchEncoder interface
func (d *MaxwellEventBatchEncoder) MixedBuild() []byte {
	keyBytes := d.keyBuf.Bytes()
	valueBytes := d.valueBuf.Bytes()
	mixedBytes := make([]byte, len(keyBytes)+len(valueBytes))
	copy(mixedBytes[:8], keyBytes[:8])
	index := uint64(8)    // skip version
	keyIndex := uint64(8) // skip version
	valueIndex := uint64(0)
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
		e.Table.TableID = *key.Partition
		e.Table.IsPartition = true
	}

	if value.Type == "delete" {
		for _, v := range e.Columns {
			v.Value = value.Data[v.Name]
		}
	} else {
		for _, v := range e.Columns {
			v.Value = value.Data[v.Name]
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

//ddl typecode from parser/model/ddl.go
func ddlToMaxwellType(ddlType model2.ActionType) string {
	if ddlType >= 5 && ddlType <= 20 {
		return "table-alter"
	}
	switch ddlType {
	case 3:
		return "table-create"
	case 4:
		return "table-drop"
	case 22, 23, 27, 28, 29, 33, 37, 38, 41, 42:
		return "table-alter"
	case 1:
		return "database-create"
	case 2:
		return "database-drop"
	case 26:
		return "database-alter"
	default:
		return ddlType.String()
	}
}

//Convert column type code to maxwell column type
func columnToMaxwellType(columnType byte) (string, error) {
	switch columnType {
	// tinyint,smallint,mediumint,int
	case 1, 2, 3, 9:
		return "int", nil
	// bigint
	case 8:
		return "bigint", nil
	// tinytext,text,mediumtext,longtext,varchar,char
	case 249, 252, 250, 251, 254, 15:
		return "string", nil
	// date
	case 10:
		return "date", nil
	// datetime,timestamp
	case 7, 12:
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
	// float,double
	case 4, 5:
		return "float", nil
	case 246:
		return "decimal", nil
	default:
		return "", errors.Errorf("unsupported column type - %v", columnType)
	}
}

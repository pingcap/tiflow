package codec

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
)

// JSONEventBatchEncoder encodes the events into the byte of a batch into.
type JSONEventBatchEncoder struct {
	keyBuf   *bytes.Buffer
	valueBuf *bytes.Buffer
}

// AppendResolvedEvent implements the EventBatchEncoder interface
func (d *JSONEventBatchEncoder) AppendResolvedEvent(ts uint64) error {
	keyMsg := model.NewResolvedMessage(ts)
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
	keyMsg, valueMsg := e.ToMqMessage()
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
	keyMsg, valueMsg := e.ToMqMessage()
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

const (
	// BatchVersion1 represents the version of batch format
	BatchVersion1 uint64 = 1
)

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
	nextKey    *model.MqMessageKey
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
	rowMsg := new(model.MqMessageRow)
	if err := rowMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	rowEvent := new(model.RowChangedEvent)
	rowEvent.FromMqMessage(b.nextKey, rowMsg)
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
	ddlMsg := new(model.MqMessageDDL)
	if err := ddlMsg.Decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent := new(model.DDLEvent)
	ddlEvent.FromMqMessage(b.nextKey, ddlMsg)
	b.nextKey = nil
	return ddlEvent, nil
}

func (b *JSONEventBatchDecoder) hasNext() bool {
	return len(b.keyBytes) > 0 && len(b.valueBytes) > 0
}

func (b *JSONEventBatchDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(model.MqMessageKey)
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

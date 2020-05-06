package codec

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
)

type DefaultEventBatchEncoder struct {
	keyBuf   *bytes.Buffer
	valueBuf *bytes.Buffer
}

func (d *DefaultEventBatchEncoder) AppendResolvedEvent(ts uint64) error {
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

func (d *DefaultEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) error {
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

func (d *DefaultEventBatchEncoder) AppendDDLEvent(e *model.DDLEvent) error {
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

func (d *DefaultEventBatchEncoder) Build() (key []byte, value []byte) {
	return d.keyBuf.Bytes(), d.valueBuf.Bytes()
}

func (d *DefaultEventBatchEncoder) Size() int {
	return d.keyBuf.Len() + d.valueBuf.Len()
}

const (
	// BatchVersion1 represents the version of batch format
	BatchVersion1 uint64 = 1
)

// NewBatchEncoder creates a new BatchEncoder.
func NewDefaultEventBatchEncoder() EventBatchEncoder {
	batch := &DefaultEventBatchEncoder{
		keyBuf:   &bytes.Buffer{},
		valueBuf: &bytes.Buffer{},
	}
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	batch.keyBuf.Write(versionByte[:])
	return batch
}

// DefaultEventBatchDecoder decodes the byte of a batch into the original messages.
type DefaultEventBatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
}

// HasNext returns whether there is a next message in the batch.
func (b *DefaultEventBatchDecoder) HasNext() bool {
	return len(b.keyBytes) > 0 && len(b.valueBytes) > 0
}

// Next returns the next message. It must be used when HasNext is true.
func (b *DefaultEventBatchDecoder) Next() ([]byte, []byte, bool) {
	if !b.HasNext() {
		return nil, nil, false
	}
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	value := b.valueBytes[8 : valueLen+8]
	b.keyBytes = b.keyBytes[keyLen+8:]
	b.valueBytes = b.valueBytes[valueLen+8:]
	return key, value, true
}

// NewBatchDecoder creates a new BatchDecoder.
func NewDefaultEventBatchDecoder(key []byte, value []byte) (*DefaultEventBatchDecoder, error) {
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != BatchVersion1 {
		return nil, errors.New("unexpected key format version")
	}
	return &DefaultEventBatchDecoder{
		keyBytes:   key,
		valueBytes: value,
	}, nil
}

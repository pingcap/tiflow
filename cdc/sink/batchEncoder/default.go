package batchEncoder

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

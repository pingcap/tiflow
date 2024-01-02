// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	"encoding/json"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

type Marshaller interface {
	// MarshalCheckpoint marshals the checkpoint ts into bytes.
	MarshalCheckpoint(ts uint64) ([]byte, error)

	// MarshalDDLEvent marshals the DDL event into bytes.
	MarshalDDLEvent(event *model.DDLEvent) ([]byte, error)

	// MarshalRowChangedEvent marshals the row changed event into bytes.
	MarshalRowChangedEvent(event *model.RowChangedEvent, config *common.Config,
		handleKeyOnly bool, claimCheckFileName string) ([]byte, error)

	// Unmarshal the bytes into the given value.
	Unmarshal(data []byte, v any) error
}

type jsonMarshaller struct{}

func newJSONMarshaller() *jsonMarshaller {
	return &jsonMarshaller{}
}

// MarshalCheckpoint implement the Marshaller interface
func (m *jsonMarshaller) MarshalCheckpoint(ts uint64) ([]byte, error) {
	msg := newResolvedMessage(ts)
	result, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return result, nil
}

// MarshalDDLEvent implement the Marshaller interface
func (m *jsonMarshaller) MarshalDDLEvent(event *model.DDLEvent) ([]byte, error) {
	var (
		msg *message
		err error
	)
	if event.IsBootstrap {
		msg, err = newBootstrapMessage(event)
	} else {
		msg, err = newDDLMessage(event)
	}
	if msg == nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return value, nil
}

// MarshalRowChangedEvent implement the Marshaller interface
func (m *jsonMarshaller) MarshalRowChangedEvent(
	event *model.RowChangedEvent, config *common.Config,
	handleKeyOnly bool,
	claimCheckFileName string,
) ([]byte, error) {
	msg, err := newDMLMessage(event, config, handleKeyOnly)
	if err != nil {
		return nil, err
	}
	msg.ClaimCheckLocation = claimCheckFileName

	value, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return value, nil
}

// Unmarshal implement the Marshaller interface
func (m *jsonMarshaller) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

type avroMarshaller struct {
	codec *goavro.Codec
}

func newAvroMarshaller(schema string) (*avroMarshaller, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &avroMarshaller{
		codec: codec,
	}, nil
}

// Marshal implement the Marshaller interface
func (m *avroMarshaller) Marshal(v any) ([]byte, error) {
	return m.codec.BinaryFromNative(nil, v)
}

// MarshalCheckpoint implement the Marshaller interface
func (m *avroMarshaller) MarshalCheckpoint(ts uint64) ([]byte, error) {
	msg := newResolvedMessageMap(ts)
	result, err := m.codec.BinaryFromNative(nil, msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return result, nil
}

// MarshalDDLEvent implement the Marshaller interface
func (m *avroMarshaller) MarshalDDLEvent(event *model.DDLEvent) ([]byte, error) {
	var msg interface{}
	if event.IsBootstrap {
		msg = newBootstrapMessageMap(event.TableInfo)
	} else {
		msg = newDDLMessageMap(event)
	}
	if msg == nil {
		return nil, nil
	}
	value, err := m.codec.BinaryFromNative(nil, msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return value, nil
}

// MarshalRowChangedEvent implement the Marshaller interface
func (m *avroMarshaller) MarshalRowChangedEvent(
	event *model.RowChangedEvent, config *common.Config,
	handleKeyOnly bool, claimCheckFileName string,
) ([]byte, error) {
	msg, err := newDMLMessageMap(event, config, handleKeyOnly, claimCheckFileName)
	if err != nil {
		return nil, err
	}
	value, err := m.codec.BinaryFromNative(nil, msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return value, nil
}

// Unmarshal implement the Marshaller interface
func (m *avroMarshaller) Unmarshal(data []byte, v any) error {
	native, _, err := m.codec.NativeFromBinary(data)
	if err != nil {
		return errors.Trace(err)
	}
	switch value := native.(type) {
	case map[string]interface{}:
		*v.(*map[string]interface{}) = value
	default:
	}
	return nil
}

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

package encoding

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
)

// SerializerDeserializer is the interface encodes and decodes model.PolymorphicEvent.
type SerializerDeserializer interface {
	Marshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error)
	Unmarshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error)
}

// MsgPackGenSerde encodes model.PolymorphicEvent into bytes and decodes
// model.PolymorphicEvent from bytes.
type MsgPackGenSerde struct{}

// Marshal encodes model.PolymorphicEvent into bytes.
func (m *MsgPackGenSerde) Marshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error) {
	bytes = bytes[:0]
	return event.RawKV.MarshalMsg(bytes)
}

// Unmarshal decodes model.PolymorphicEvent from bytes.
func (m *MsgPackGenSerde) Unmarshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error) {
	if event.RawKV == nil {
		event.RawKV = new(model.RawKVEntry)
	}

	bytes, err := event.RawKV.UnmarshalMsg(bytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	event.StartTs = event.RawKV.StartTs
	event.CRTs = event.RawKV.CRTs

	return bytes, nil
}

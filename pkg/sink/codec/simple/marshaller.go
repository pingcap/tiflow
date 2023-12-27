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
	"github.com/pingcap/tiflow/pkg/errors"
)

type Marshaller interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonMarshaller struct {
}

func newJSONMarshaller() *jsonMarshaller {
	return &jsonMarshaller{}
}

func (m *jsonMarshaller) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

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

func (m *avroMarshaller) Marshal(v any) ([]byte, error) {
	return m.codec.BinaryFromNative(nil, v)
}

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

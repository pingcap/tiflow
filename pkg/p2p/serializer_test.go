// Copyright 2021 PingCAP, Inc.
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

package p2p

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type jsonSerializableMessage struct {
	A int
	B float64
	C string
}

type msgpackSerializableMessage struct {
	A int
	B float64
	C string
	D []int
}

func (m *msgpackSerializableMessage) Marshal() ([]byte, error) {
	return msgpack.Marshal(m)
}

func (m *msgpackSerializableMessage) Unmarshal(data []byte) error {
	return msgpack.Unmarshal(data, m)
}

func TestJsonSerializable(t *testing.T) {
	msg := &jsonSerializableMessage{
		A: 1,
		B: 2,
		C: "test",
	}

	data, err := marshalMessage(msg)
	require.NoError(t, err)

	msg1 := &jsonSerializableMessage{}
	err = unmarshalMessage(data, msg1)
	require.NoError(t, err)

	require.True(t, reflect.DeepEqual(msg, msg1))
}

func TestMsgpackSerializable(t *testing.T) {
	msg := &msgpackSerializableMessage{
		A: 1,
		B: 2,
		C: "test",
		D: []int{1, 2, 3, 4, 5, 6},
	}
	data, err := marshalMessage(msg)
	require.NoError(t, err)

	data1, err := msgpack.Marshal(msg)
	require.NoError(t, err)
	require.Equal(t, data1, data)

	msg1 := &msgpackSerializableMessage{}
	err = unmarshalMessage(data, msg1)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(msg, msg1))
}

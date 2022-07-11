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
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	canal "github.com/pingcap/tiflow/proto/canal"
	"github.com/stretchr/testify/require"
)

func TestCanalBatchEncoder(t *testing.T) {
	t.Parallel()
	s := defaultCanalBatchTester
	for _, cs := range s.rowCases {
		encoder := newCanalBatchEncoder()
		for _, row := range cs {
			err := encoder.AppendRowChangedEvent(context.Background(), "", row, nil)
			require.Nil(t, err)
		}
		res := encoder.Build()

		if len(cs) == 0 {
			require.Nil(t, res)
			continue
		}

		require.Len(t, res, 1)
		require.Nil(t, res[0].Key)
		require.Equal(t, len(cs), res[0].GetRowsCount())

		packet := &canal.Packet{}
		err := proto.Unmarshal(res[0].Value, packet)
		require.Nil(t, err)
		require.Equal(t, canal.PacketType_MESSAGES, packet.GetType())
		messages := &canal.Messages{}
		err = proto.Unmarshal(packet.GetBody(), messages)
		require.Nil(t, err)
		require.Equal(t, len(cs), len(messages.GetMessages()))
	}

	for _, cs := range s.ddlCases {
		encoder := newCanalBatchEncoder()
		for _, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.Nil(t, err)
			require.NotNil(t, msg)
			require.Nil(t, msg.Key)

			packet := &canal.Packet{}
			err = proto.Unmarshal(msg.Value, packet)
			require.Nil(t, err)
			require.Equal(t, canal.PacketType_MESSAGES, packet.GetType())
			messages := &canal.Messages{}
			err = proto.Unmarshal(packet.GetBody(), messages)
			require.Nil(t, err)
			require.Equal(t, 1, len(messages.GetMessages()))
			require.Nil(t, err)
		}
	}
}

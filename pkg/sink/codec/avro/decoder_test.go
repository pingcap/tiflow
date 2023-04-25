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

package avro

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestDecodeEvent(t *testing.T) {
	o := &Options{
		EnableTiDBExtension:        true,
		DecimalHandlingMode:        "precise",
		BigintUnsignedHandlingMode: "long",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := setupEncoderAndSchemaRegistry(ctx, o)
	require.NoError(t, err)
	defer teardownEncoderAndSchemaRegistry()

	cols := make([]*model.Column, 0)
	colInfos := make([]rowcodec.ColInfo, 0)

	cols = append(
		cols,
		&model.Column{
			Name:  "id",
			Value: int64(1),
			Type:  mysql.TypeLong,
			Flag:  model.HandleKeyFlag,
		},
	)
	colInfos = append(
		colInfos,
		rowcodec.ColInfo{
			ID:            1000,
			IsPKHandle:    true,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeLong),
		},
	)

	for _, v := range avroTestColumns {
		cols = append(cols, &v.col)
		colInfos = append(colInfos, v.colInfo)

		colNew := v.col
		colNew.Name = colNew.Name + "nullable"
		colNew.Value = nil
		colNew.Flag.SetIsNullable()

		colInfoNew := v.colInfo
		colInfoNew.ID += int64(len(avroTestColumns))

		cols = append(cols, &colNew)
		colInfos = append(colInfos, colInfoNew)
	}

	input := &avroEncodeInput{
		cols,
		colInfos,
	}

	rand.New(rand.NewSource(time.Now().Unix())).Shuffle(len(input.columns), func(i, j int) {
		input.columns[i], input.columns[j] = input.columns[j], input.columns[i]
		input.colInfos[i], input.colInfos[j] = input.colInfos[j], input.colInfos[i]
	})

	// insert event
	event := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "avro",
		},
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "test",
				Table:  "avro",
			},
		},
		Columns:  input.columns,
		ColInfos: input.colInfos,
	}

	topic := "avro-test-topic"
	err = encoder.AppendRowChangedEvent(ctx, topic, event, func() {})
	require.NoError(t, err)

	messages := encoder.Build()
	require.Len(t, messages, 1)
	message := messages[0]

	keySchemaM, valueSchemaM, err := newSchemaManager4Test(ctx)
	require.NoError(t, err)

	decoder := NewDecoder(message.Key, message.Value, o, keySchemaM, valueSchemaM, topic)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeRow, messageType)

	decodedEvent, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedEvent)
}

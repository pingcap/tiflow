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

	"github.com/linkedin/goavro/v2"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/stretchr/testify/require"
)

func TestDMLEventE2E(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, event, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
	colInfos := event.TableInfo.GetColInfosForRowChangedEvent()

	rand.New(rand.NewSource(time.Now().Unix())).Shuffle(len(event.Columns), func(i, j int) {
		event.Columns[i], event.Columns[j] = event.Columns[j], event.Columns[i]
		colInfos[i], colInfos[j] = colInfos[j], colInfos[i]
	})

	for _, decimalHandling := range []string{"precise", "string"} {
		for _, unsignedBigintHandling := range []string{"long", "string"} {
			codecConfig.AvroDecimalHandlingMode = decimalHandling
			codecConfig.AvroBigintUnsignedHandlingMode = unsignedBigintHandling

			encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
			require.NoError(t, err)
			require.NotNil(t, encoder)

			topic := "avro-test-topic"
			err = encoder.AppendRowChangedEvent(ctx, topic, event, func() {})
			require.NoError(t, err)

			messages := encoder.Build()
			require.Len(t, messages, 1)
			message := messages[0]

			schemaM, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
			require.NoError(t, err)

			decoder := NewDecoder(codecConfig, schemaM, topic, nil)
			err = decoder.AddKeyValue(message.Key, message.Value)
			require.NoError(t, err)

			messageType, exist, err := decoder.HasNext()
			require.NoError(t, err)
			require.True(t, exist)
			require.Equal(t, model.MessageTypeRow, messageType)

			decodedEvent, err := decoder.NextRowChangedEvent()
			require.NoError(t, err)
			require.NotNil(t, decodedEvent)

			TeardownEncoderAndSchemaRegistry4Testing()
		}
	}
}

func TestDDLEventE2E(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.AvroEnableWatermark = true

	encoder := NewAvroEncoder(model.DefaultNamespace, nil, codecConfig)

	ddl, _, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
	message, err := encoder.EncodeDDLEvent(ddl)
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	decoder := NewDecoder(codecConfig, nil, topic, nil)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeDDL, messageType)

	decodedEvent, err := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedEvent)
	require.Equal(t, ddl.CommitTs, decodedEvent.CommitTs)
	require.Equal(t, timodel.ActionCreateTable, decodedEvent.Type)
	require.NotEmpty(t, decodedEvent.Query)
	require.NotEmpty(t, decodedEvent.TableInfo.TableName.Schema)
	require.NotEmpty(t, decodedEvent.TableInfo.TableName.Table)
}

func TestResolvedE2E(t *testing.T) {
	t.Parallel()

	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.AvroEnableWatermark = true

	encoder := NewAvroEncoder(model.DefaultNamespace, nil, codecConfig)

	resolvedTs := uint64(1591943372224)
	message, err := encoder.EncodeCheckpointEvent(resolvedTs)
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	decoder := NewDecoder(codecConfig, nil, topic, nil)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeResolved, messageType)

	obtained, err := decoder.NextResolvedEvent()
	require.NoError(t, err)
	require.Equal(t, resolvedTs, obtained)
}

func TestAvroEncode4EnableChecksum(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.EnableRowChecksum = true
	codecConfig.AvroDecimalHandlingMode = "string"
	codecConfig.AvroBigintUnsignedHandlingMode = "string"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	_, event, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
	topic := "default"
	bin, err := encoder.encodeValue(ctx, "default", event)
	require.NoError(t, err)

	cid, data, err := extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroValueCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err := avroValueCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)

	m, ok := res.(map[string]interface{})
	require.True(t, ok)

	_, found := m[tidbRowLevelChecksum]
	require.True(t, found)

	_, found = m[tidbCorrupted]
	require.True(t, found)

	_, found = m[tidbChecksumVersion]
	require.True(t, found)
}

func TestAvroEncode(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	_, event, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
	topic := "default"
	bin, err := encoder.encodeKey(ctx, topic, event)
	require.NoError(t, err)

	cid, data, err := extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroKeyCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err := avroKeyCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)
	for k := range res.(map[string]interface{}) {
		if k == "_tidb_commit_ts" || k == "_tidb_op" || k == "_tidb_commit_physical_time" {
			require.Fail(t, "key shall not include extension fields")
		}
	}
	require.Equal(t, int32(127), res.(map[string]interface{})["tu1"])

	bin, err = encoder.encodeValue(ctx, topic, event)
	require.NoError(t, err)

	cid, data, err = extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroValueCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err = avroValueCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)

	for k, v := range res.(map[string]interface{}) {
		if k == "_tidb_op" {
			require.Equal(t, "c", v.(string))
		}
		if k == "float" {
			require.Equal(t, float32(3.14), v)
		}
	}
}

func TestAvroEnvelope(t *testing.T) {
	t.Parallel()
	cManager := &confluentSchemaManager{}
	gManager := &glueSchemaManager{}
	avroCodec, err := goavro.NewCodec(`
       {
         "type": "record",
         "name": "testdb.avroenvelope",
         "fields" : [
           {"name": "id", "type": "int", "default": 0}
         ]
       }`)

	require.NoError(t, err)

	testNativeData := make(map[string]interface{})
	testNativeData["id"] = 7

	bin, err := avroCodec.BinaryFromNative(nil, testNativeData)
	require.NoError(t, err)

	// test confluent schema message
	header, err := cManager.getMsgHeader(8)
	require.NoError(t, err)
	res := avroEncodeResult{
		data:   bin,
		header: header,
	}

	evlp, err := res.toEnvelope()
	require.NoError(t, err)
	require.Equal(t, header, evlp[0:5])

	parsed, _, err := avroCodec.NativeFromBinary(evlp[5:])
	require.NoError(t, err)
	require.NotNil(t, parsed)

	id, exists := parsed.(map[string]interface{})["id"]
	require.True(t, exists)
	require.Equal(t, int32(7), id)

	// test glue schema message
	uuidGenerator := uuid.NewGenerator()
	uuidS := uuidGenerator.NewString()
	header, err = gManager.getMsgHeader(uuidS)
	require.NoError(t, err)
	res = avroEncodeResult{
		data:   bin,
		header: header,
	}
	evlp, err = res.toEnvelope()
	require.NoError(t, err)
	require.Equal(t, header, evlp[0:18])

	parsed, _, err = avroCodec.NativeFromBinary(evlp[18:])
	require.NoError(t, err)
	require.NotNil(t, parsed)
	id, exists = parsed.(map[string]interface{})["id"]
	require.True(t, exists)
	require.Equal(t, int32(7), id)
}

func TestSanitizeName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "normalColumnName123", common.SanitizeName("normalColumnName123"))
	require.Equal(
		t,
		"_1ColumnNameStartWithNumber",
		common.SanitizeName("1ColumnNameStartWithNumber"),
	)
	require.Equal(t, "A_B", common.SanitizeName("A.B"))
	require.Equal(t, "columnNameWith__", common.SanitizeName("columnNameWith中文"))
}

func TestGetAvroNamespace(t *testing.T) {
	t.Parallel()

	require.Equal(
		t,
		"normalNamespace.normalSchema",
		getAvroNamespace("normalNamespace", "normalSchema"),
	)
	require.Equal(
		t,
		"_1Namespace._1Schema",
		getAvroNamespace("1Namespace", "1Schema"),
	)
	require.Equal(
		t,
		"N_amespace.S_chema",
		getAvroNamespace("N-amespace", "S.chema"),
	)

	require.Equal(
		t,
		"normalNamespace",
		getAvroNamespace("normalNamespace", ""),
	)
}

func TestArvoAppendRowChangedEventWithCallback(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	cols := []*model.Column{{
		Name: "col1",
		Type: mysql.TypeVarchar,
		Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
	}}
	tableInfo := model.BuildTableInfo("a", "b", cols, [][]int{{0}})
	row := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		Columns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "col1",
			Value: []byte("aa"),
		}}, tableInfo),
	}

	expected := 0
	count := 0
	for i := 0; i < 5; i++ {
		expected += i
		bit := i
		err := encoder.AppendRowChangedEvent(ctx, "", row, func() {
			count += bit
		})
		require.NoError(t, err)

		msgs = encoder.Build()
		require.Len(t, msgs, 1, "one message should be built")

		msgs[0].Callback()
		require.Equal(t, expected, count, "expected one callback be called")
	}
}

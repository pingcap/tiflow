// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"context"
	"encoding/json"
	"math/big"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/sink/codec/avro"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/oracle"
)

type SQLTestHelper struct {
	t *testing.T

	helper  *entry.SchemaTestHelper
	mounter entry.Mounter

	ts      uint64
	tableID int64
}

func NewSQLTestHelper(t *testing.T, tableName, initialCreateTableDDL string) *SQLTestHelper {
	helper := entry.NewSchemaTestHelper(t)
	helper.Tk().MustExec("set @@tidb_enable_clustered_index=1;")
	helper.Tk().MustExec("use test;")

	changefeed := model.DefaultChangeFeedID("")

	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.NoError(t, err)

	cfg := config.GetDefaultReplicaConfig()

	filter, err := filter.NewFilter(cfg, "")
	require.NoError(t, err)

	schemaStorage, err := entry.NewSchemaStorage(helper.Storage(),
		ver.Ver, false, changefeed, util.RoleTester, filter)
	require.NoError(t, err)

	job := helper.DDL2Job(initialCreateTableDDL)
	err = schemaStorage.HandleDDLJob(job)
	require.NoError(t, err)

	ts := schemaStorage.GetLastSnapshot().CurrentTs()
	schemaStorage.AdvanceResolvedTs(ver.Ver)

	mounter := entry.NewMounter(schemaStorage, changefeed, time.UTC, filter, cfg.Integrity)

	tableInfo, ok := schemaStorage.GetLastSnapshot().TableByName("test", tableName)
	require.True(t, ok)

	return &SQLTestHelper{
		t:       t,
		helper:  helper,
		mounter: mounter,
		ts:      ts,
		tableID: tableInfo.ID,
	}
}

func (h *SQLTestHelper) Close() {
	h.helper.Close()
}

func (h *SQLTestHelper) MustExec(query string, args ...interface{}) {
	h.helper.Tk().MustExec(query, args...)
}

func (h *SQLTestHelper) ScanTable() []*model.RowChangedEvent {
	txn, err := h.helper.Storage().Begin()
	require.Nil(h.t, err)
	defer txn.Rollback() //nolint:errcheck
	startKey, endKey := spanz.GetTableRange(h.tableID)
	kvIter, err := txn.Iter(startKey, endKey)
	require.Nil(h.t, err)
	defer kvIter.Close()

	ret := make([]*model.RowChangedEvent, 0)

	for kvIter.Valid() {
		rawKV := &model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     kvIter.Key(),
			Value:   kvIter.Value(),
			StartTs: h.ts - 1,
			CRTs:    h.ts + 1,
		}
		pEvent := model.NewPolymorphicEvent(rawKV)
		err := h.mounter.DecodeEvent(context.Background(), pEvent)
		require.Nil(h.t, err)
		if pEvent.Row == nil {
			return ret
		}

		row := pEvent.Row
		ret = append(ret, row)

		err = kvIter.Next()
		require.Nil(h.t, err)
	}

	return ret
}

type debeziumSuite struct {
	suite.Suite
	disableSchema bool
}

func (s *debeziumSuite) requireDebeziumJSONEq(dbzOutput []byte, tiCDCOutput []byte) {
	var (
		ignoredRecordPaths = map[string]bool{
			`{map[string]any}["schema"]`:                             s.disableSchema,
			`{map[string]any}["payload"].(map[string]any)["source"]`: true,
			`{map[string]any}["payload"].(map[string]any)["ts_ms"]`:  true,
		}

		compareOpt = cmp.FilterPath(
			func(p cmp.Path) bool {
				path := p.GoString()
				_, shouldIgnore := ignoredRecordPaths[path]
				return shouldIgnore
			},
			cmp.Ignore(),
		)
	)

	var objDbzOutput map[string]any
	s.Require().Nil(json.Unmarshal(dbzOutput, &objDbzOutput), "Failed to unmarshal Debezium JSON")

	var objTiCDCOutput map[string]any
	s.Require().Nil(json.Unmarshal(tiCDCOutput, &objTiCDCOutput), "Failed to unmarshal TiCDC JSON")

	if diff := cmp.Diff(objDbzOutput, objTiCDCOutput, compareOpt); diff != "" {
		s.Failf("JSON is not equal", "Diff (-debezium, +ticdc):\n%s", diff)
	}
}

func TestDebeziumSuiteEnableSchema(t *testing.T) {
	suite.Run(t, &debeziumSuite{
		disableSchema: false,
	})
}

func TestDebeziumSuiteDisableSchema(t *testing.T) {
	suite.Run(t, &debeziumSuite{
		disableSchema: true,
	})
}

func (s *debeziumSuite) TestDataTypes() {
	dataDDL, err := os.ReadFile("testdata/datatype.ddl.sql")
	s.Require().Nil(err)

	dataDML, err := os.ReadFile("testdata/datatype.dml.sql")
	s.Require().Nil(err)

	dataDbzOutput, err := os.ReadFile("testdata/datatype.dbz.json")
	s.Require().Nil(err)

	helper := NewSQLTestHelper(s.T(), "foo", string(dataDDL))

	helper.MustExec(`SET sql_mode='';`)
	helper.MustExec(`SET time_zone='UTC';`)
	helper.MustExec(string(dataDML))

	rows := helper.ScanTable()
	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.TimeZone = time.UTC
	cfg.DebeziumDisableSchema = s.disableSchema
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	builder, err := NewBatchEncoderBuilder(ctx, cfg, "dbserver1")
	s.Require().Nil(err)
	encoder := builder.Build()
	for _, row := range rows {
		err := encoder.AppendRowChangedEvent(context.Background(), "", row, nil)
		s.Require().Nil(err)
	}

	messages := encoder.Build()
	s.Require().Len(messages, 1)
	s.requireDebeziumJSONEq(dataDbzOutput, messages[0].Value)
}

func TestDMLEventE2E(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolDebezium)
	codecConfig.EncodingFormat = common.EncodingFormatAvro
	codecConfig.EnableTiDBExtension = false
	codecConfig.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
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
			for _, timePrecisionHandling := range []string{"adaptive_time_microseconds", "string"} {
				codecConfig.AvroDecimalHandlingMode = decimalHandling
				codecConfig.AvroBigintUnsignedHandlingMode = unsignedBigintHandling
				codecConfig.AvroTimePrecisionHandlingMode = timePrecisionHandling

				encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
				require.NoError(t, err)
				require.NotNil(t, encoder)

				// debezium topic name format: topic_prefix.schema_name.table_name
				topic := "avro-test-topic.test.t"
				err = encoder.AppendRowChangedEvent(ctx, topic, event, func() {})
				require.NoError(t, err)

				messages := encoder.Build()
				require.Len(t, messages, 1)
				message := messages[0]

				schemaM, err := avro.NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
				require.NoError(t, err)

				var key, value, schema map[string]interface{}
				key, _, err = avro.DecodeRawBytes(ctx, schemaM, message.Key, topic)
				require.NoError(t, err)
				require.Equal(t, int32(127), key["tu1"])
				value, schema, err = avro.DecodeRawBytes(ctx, schemaM, message.Value, topic)
				require.NoError(t, err)
				namespace := schema["namespace"].(string)
				after := value["after"].(map[string]interface{})[namespace+".Value"].(map[string]interface{})
				require.Equal(t, float32(3.14), after["floatT"].(map[string]interface{})["float"])
				if decimalHandling == "precise" {
					v, bl := new(big.Rat).SetString("2333.654321")
					require.Equal(t, true, bl)
					require.Equal(t, v, after["decimalT"].(map[string]interface{})["bytes.decimal"])
					v, bl = new(big.Rat).SetString("2333.123456")
					require.Equal(t, true, bl)
					require.Equal(t, v, after["decimalTu"].(map[string]interface{})["bytes.decimal"])
					v, bl = new(big.Rat).SetString("1.7371")
					require.Equal(t, true, bl)
					require.Equal(t, v, after["decimalTu2"].(map[string]interface{})["bytes.decimal"])
				} else {
					require.Equal(t, "2333.654321", after["decimalT"].(map[string]interface{})["string"])
					require.Equal(t, "2333.123456", after["decimalTu"].(map[string]interface{})["string"])
					require.Equal(t, "1.7371", after["decimalTu2"].(map[string]interface{})["string"])
				}
				if unsignedBigintHandling == "string" {
					require.Equal(t, "9223372036854775807", after["biu1"].(map[string]interface{})["string"])
					require.Equal(t, "9223372036854775808", after["biu2"].(map[string]interface{})["string"])
					require.Equal(t, "0", after["biu3"].(map[string]interface{})["string"])
					require.Equal(t, nil, after["biu4"])
				} else {
					require.Equal(t, int64(9223372036854775807), after["biu1"].(map[string]interface{})["long"])
					require.Equal(t, int64(-9223372036854775808), after["biu2"].(map[string]interface{})["long"])
					require.Equal(t, int64(0), after["biu3"].(map[string]interface{})["long"])
					require.Equal(t, nil, after["biu4"])
				}
				if timePrecisionHandling == "adaptive_time_microseconds" {
					require.Equal(t, int32(18312), after["dateT"].(map[string]interface{})["int"])
					require.Equal(t, int64(1582165220000), after["datetimeT"].(map[string]interface{})["long"])
					require.Equal(t, int64(8420000), after["timeT"].(map[string]interface{})["long"])
				} else {
					require.Equal(t, "2020-02-20", after["dateT"].(map[string]interface{})["string"])
					require.Equal(t, "2020-02-20 02:20:20", after["datetimeT"].(map[string]interface{})["string"])
					require.Equal(t, "02:20:20", after["timeT"].(map[string]interface{})["string"])
				}

				decoder := avro.NewDecoder(codecConfig, schemaM, topic, nil)
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
}

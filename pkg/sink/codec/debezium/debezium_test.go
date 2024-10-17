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
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
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
	keyDbzOutput, err := os.ReadFile("testdata/datatype.dbz.key.json")
	s.Require().Nil(err)

	helper := NewSQLTestHelper(s.T(), "foo", string(dataDDL))

	helper.MustExec(`SET sql_mode='';`)
	helper.MustExec(`SET time_zone='UTC';`)
	helper.MustExec(string(dataDML))

	rows := helper.ScanTable()
	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.TimeZone = time.UTC
	cfg.DebeziumDisableSchema = s.disableSchema
	encoder := NewBatchEncoderBuilder(cfg, "dbserver1").Build()
	for _, row := range rows {
		err := encoder.AppendRowChangedEvent(context.Background(), "", row, nil)
		s.Require().Nil(err)
	}

	messages := encoder.Build()
	s.Require().Len(messages, 1)
	s.requireDebeziumJSONEq(dataDbzOutput, messages[0].Value)
	s.requireDebeziumJSONEq(keyDbzOutput, messages[0].Key)
}

// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	 http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package entry

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	ticonfig "github.com/pingcap/tidb/pkg/config"
	tiddl "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	timeta "github.com/pingcap/tidb/pkg/meta"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

// SchemaTestHelper is a test helper for schema which creates an internal tidb instance to generate DDL jobs with meta information
type SchemaTestHelper struct {
	t       *testing.T
	tk      *testkit.TestKit
	storage kv.Storage
	domain  *domain.Domain

	schemaStorage SchemaStorage
	mounter       Mounter
	filter        filter.Filter
}

// NewSchemaTestHelperWithReplicaConfig creates a SchemaTestHelper
// by using the given replica config.
func NewSchemaTestHelperWithReplicaConfig(
	t *testing.T, replicaConfig *config.ReplicaConfig,
) *SchemaTestHelper {
	store, err := mockstore.NewMockStore()
	require.Nil(t, err)
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		conf.AlterPrimaryKey = true
	})
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	require.Nil(t, err)
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(t, store)

	filter, err := filter.NewFilter(replicaConfig, "")
	require.NoError(t, err)

	ver, err := store.CurrentVersion(oracle.GlobalTxnScope)
	require.NoError(t, err)

	changefeedID := model.DefaultChangeFeedID("changefeed-testkit")

	meta := timeta.NewSnapshotMeta(store.GetSnapshot(ver))
	schemaStorage, err := NewSchemaStorage(
		meta, ver.Ver, replicaConfig.ForceReplicate,
		changefeedID, util.RoleTester, filter)
	require.NoError(t, err)

	mounter := NewMounter(schemaStorage, changefeedID, time.Local,
		filter, replicaConfig.Integrity)

	return &SchemaTestHelper{
		t:             t,
		tk:            tk,
		storage:       store,
		domain:        domain,
		filter:        filter,
		schemaStorage: schemaStorage,
		mounter:       mounter,
	}
}

// NewSchemaTestHelper creates a SchemaTestHelper
func NewSchemaTestHelper(t *testing.T) *SchemaTestHelper {
	return NewSchemaTestHelperWithReplicaConfig(t, config.GetDefaultReplicaConfig())
}

// DDL2Job executes the DDL stmt and returns the DDL job
func (s *SchemaTestHelper) DDL2Job(ddl string) *timodel.Job {
	s.tk.MustExec(ddl)
	jobs, err := tiddl.GetLastNHistoryDDLJobs(s.GetCurrentMeta(), 1)
	require.Nil(s.t, err)
	require.Len(s.t, jobs, 1)
	// Set State from Synced to Done.
	// Because jobs are put to history queue after TiDB alter its state from
	// Done to Synced.
	jobs[0].State = timodel.JobStateDone
	res := jobs[0]
	if res.Type != timodel.ActionRenameTables {
		return res
	}

	// the RawArgs field in job fetched from tidb snapshot meta is incorrent,
	// so we manually construct `job.RawArgs` to do the workaround.
	// we assume the old schema name is same as the new schema name here.
	// for example, "ALTER TABLE RENAME test.t1 TO test.t1, test.t2 to test.t22", schema name is "test"
	schema := strings.Split(strings.Split(strings.Split(res.Query, ",")[1], " ")[1], ".")[0]
	tableNum := len(res.BinlogInfo.MultipleTableInfos)
	oldSchemaIDs := make([]int64, tableNum)
	for i := 0; i < tableNum; i++ {
		oldSchemaIDs[i] = res.SchemaID
	}
	oldTableIDs := make([]int64, tableNum)
	for i := 0; i < tableNum; i++ {
		oldTableIDs[i] = res.BinlogInfo.MultipleTableInfos[i].ID
	}
	newTableNames := make([]timodel.CIStr, tableNum)
	for i := 0; i < tableNum; i++ {
		newTableNames[i] = res.BinlogInfo.MultipleTableInfos[i].Name
	}
	oldSchemaNames := make([]timodel.CIStr, tableNum)
	for i := 0; i < tableNum; i++ {
		oldSchemaNames[i] = timodel.NewCIStr(schema)
	}
	newSchemaIDs := oldSchemaIDs

	args := []interface{}{
		oldSchemaIDs, newSchemaIDs,
		newTableNames, oldTableIDs, oldSchemaNames,
	}
	rawArgs, err := json.Marshal(args)
	require.NoError(s.t, err)
	res.RawArgs = rawArgs
	return res
}

// DDL2Jobs executes the DDL statement and return the corresponding DDL jobs.
// It is mainly used for "DROP TABLE" and "DROP VIEW" statement because
// multiple jobs will be generated after executing these two types of
// DDL statements.
func (s *SchemaTestHelper) DDL2Jobs(ddl string, jobCnt int) []*timodel.Job {
	s.tk.MustExec(ddl)
	jobs, err := tiddl.GetLastNHistoryDDLJobs(s.GetCurrentMeta(), jobCnt)
	require.Nil(s.t, err)
	require.Len(s.t, jobs, jobCnt)
	// Set State from Synced to Done.
	// Because jobs are put to history queue after TiDB alter its state from
	// Done to Synced.
	for i := range jobs {
		jobs[i].State = timodel.JobStateDone
	}
	return jobs
}

// DML2Event execute the dml and return the corresponding row changed event.
// caution: it does not support `delete` since the key value cannot be found
// after the query executed.
func (s *SchemaTestHelper) DML2Event(dml string, schema, table string) *model.RowChangedEvent {
	s.tk.MustExec(dml)

	tableInfo, ok := s.schemaStorage.GetLastSnapshot().TableByName(schema, table)
	require.True(s.t, ok)

	key, value := s.getLastKeyValue(tableInfo.ID)
	ts := s.schemaStorage.GetLastSnapshot().CurrentTs()
	rawKV := &model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      key,
		Value:    value,
		OldValue: nil,
		StartTs:  ts - 1,
		CRTs:     ts + 1,
	}
	polymorphicEvent := model.NewPolymorphicEvent(rawKV)
	err := s.mounter.DecodeEvent(context.Background(), polymorphicEvent)
	require.NoError(s.t, err)
	return polymorphicEvent.Row
}

func (s *SchemaTestHelper) getLastKeyValue(tableID int64) (key, value []byte) {
	txn, err := s.storage.Begin()
	require.NoError(s.t, err)
	defer txn.Rollback() //nolint:errcheck

	start, end := spanz.GetTableRange(tableID)
	iter, err := txn.Iter(start, end)
	require.NoError(s.t, err)
	defer iter.Close()
	for iter.Valid() {
		key = iter.Key()
		value = iter.Value()
		err = iter.Next()
		require.NoError(s.t, err)
	}
	return key, value
}

// DDL2Event executes the DDL and return the corresponding event.
func (s *SchemaTestHelper) DDL2Event(ddl string) *model.DDLEvent {
	s.tk.MustExec(ddl)
	jobs, err := tiddl.GetLastNHistoryDDLJobs(s.GetCurrentMeta(), 1)
	require.NoError(s.t, err)
	require.Len(s.t, jobs, 1)
	// Set State from Synced to Done.
	// Because jobs are put to history queue after TiDB alter its state from
	// Done to Synced.
	jobs[0].State = timodel.JobStateDone
	res := jobs[0]
	if res.Type == timodel.ActionRenameTables {
		// the RawArgs field in job fetched from tidb snapshot meta is incorrent,
		// so we manually construct `job.RawArgs` to do the workaround.
		// we assume the old schema name is same as the new schema name here.
		// for example, "ALTER TABLE RENAME test.t1 TO test.t1, test.t2 to test.t22", schema name is "test"
		schema := strings.Split(strings.Split(strings.Split(res.Query, ",")[1], " ")[1], ".")[0]
		tableNum := len(res.BinlogInfo.MultipleTableInfos)
		oldSchemaIDs := make([]int64, tableNum)
		for i := 0; i < tableNum; i++ {
			oldSchemaIDs[i] = res.SchemaID
		}
		oldTableIDs := make([]int64, tableNum)
		for i := 0; i < tableNum; i++ {
			oldTableIDs[i] = res.BinlogInfo.MultipleTableInfos[i].ID
		}
		newTableNames := make([]timodel.CIStr, tableNum)
		for i := 0; i < tableNum; i++ {
			newTableNames[i] = res.BinlogInfo.MultipleTableInfos[i].Name
		}
		oldSchemaNames := make([]timodel.CIStr, tableNum)
		for i := 0; i < tableNum; i++ {
			oldSchemaNames[i] = timodel.NewCIStr(schema)
		}
		newSchemaIDs := oldSchemaIDs

		args := []interface{}{
			oldSchemaIDs, newSchemaIDs,
			newTableNames, oldTableIDs, oldSchemaNames,
		}
		rawArgs, err := json.Marshal(args)
		require.NoError(s.t, err)
		res.RawArgs = rawArgs
	}

	err = s.schemaStorage.HandleDDLJob(res)
	require.NoError(s.t, err)

	ver, err := s.storage.CurrentVersion(oracle.GlobalTxnScope)
	require.NoError(s.t, err)
	s.schemaStorage.AdvanceResolvedTs(ver.Ver)

	var tableInfo *model.TableInfo
	if res.BinlogInfo != nil && res.BinlogInfo.TableInfo != nil {
		tableInfo = model.WrapTableInfo(res.SchemaID, res.SchemaName, res.BinlogInfo.FinishedTS, res.BinlogInfo.TableInfo)
	} else {
		tableInfo = &model.TableInfo{
			TableName: model.TableName{Schema: res.SchemaName},
			Version:   res.BinlogInfo.FinishedTS,
		}
	}
	ctx := context.Background()
	snap, err := s.schemaStorage.GetSnapshot(ctx, res.BinlogInfo.FinishedTS-1)
	require.NoError(s.t, err)
	preTableInfo, err := snap.PreTableInfo(res)
	require.NoError(s.t, err)

	event := &model.DDLEvent{
		StartTs:      res.StartTS,
		CommitTs:     res.BinlogInfo.FinishedTS,
		TableInfo:    tableInfo,
		PreTableInfo: preTableInfo,
		Query:        res.Query,
		Type:         res.Type,
	}

	return event
}

// Storage returns the tikv storage
func (s *SchemaTestHelper) Storage() kv.Storage {
	return s.storage
}

// Tk returns the TestKit
func (s *SchemaTestHelper) Tk() *testkit.TestKit {
	return s.tk
}

// GetCurrentMeta return the current meta snapshot
func (s *SchemaTestHelper) GetCurrentMeta() *timeta.Meta {
	ver, err := s.storage.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(s.t, err)
	return timeta.NewSnapshotMeta(s.storage.GetSnapshot(ver))
}

// Close closes the helper
func (s *SchemaTestHelper) Close() {
	s.domain.Close()
	s.storage.Close() //nolint:errcheck
}

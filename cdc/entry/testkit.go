// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/log"
	ticonfig "github.com/pingcap/tidb/config"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type TestKit struct {
	*testkit.TestKit
	t       *testing.T
	storage kv.Storage
	domain  *domain.Domain

	schemaStorage SchemaStorage
	mounter       Mounter
	filter        filter.Filter
}

// NewTestKit return a new testkit
func NewTestKit(t *testing.T, replicaConfig *config.ReplicaConfig) *TestKit {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		conf.AlterPrimaryKey = true
	})
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
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

	return &TestKit{
		t:             t,
		TestKit:       tk,
		storage:       store,
		domain:        domain,
		filter:        filter,
		schemaStorage: schemaStorage,
		mounter:       mounter,
	}
}

func (tk *TestKit) GetAllEventsByTable(schema, table string) []*model.RowChangedEvent {
	tableInfo, ok := tk.schemaStorage.GetLastSnapshot().TableByName(schema, table)
	require.True(tk.t, ok)

	var tableIDs []int64
	tableIDs = append(tableIDs, tableInfo.ID)
	partitionInfo := tableInfo.GetPartitionInfo()
	if partitionInfo != nil {
		for _, partition := range partitionInfo.Definitions {
			tableIDs = append(tableIDs, partition.ID)
		}
	}

	result := make([]*model.RowChangedEvent, 0)
	for _, tableID := range tableIDs {
		tk.iterKeyValue(tableID, func(key []byte, value []byte) {
			ts := tk.schemaStorage.GetLastSnapshot().CurrentTs()
			rawKV := &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      key,
				Value:    value,
				OldValue: nil,
				StartTs:  ts - 1,
				CRTs:     ts + 1,
			}
			polymorphicEvent := model.NewPolymorphicEvent(rawKV)
			err := tk.mounter.DecodeEvent(context.Background(), polymorphicEvent)
			require.NoError(tk.t, err)
			result = append(result, polymorphicEvent.Row)
		})
	}
	return result
}

func (tk *TestKit) DML2Event(dml string, schema, table string) *model.RowChangedEvent {
	tk.MustExec(dml)

	tableInfo, ok := tk.schemaStorage.GetLastSnapshot().TableByName(schema, table)
	require.True(tk.t, ok)

	//	if partitionInfo == nil {
	//		return mountAndCheckRowInTable(tableInfo.ID, rowsBytes[0], f)
	//	}
	//	var rows int
	//	for i, p := range partitionInfo.Definitions {
	//		rows += mountAndCheckRowInTable(p.ID, rowsBytes[i], f)
	//	}

	key, value := tk.getLastKeyValue(tableInfo.ID)
	ts := tk.schemaStorage.GetLastSnapshot().CurrentTs()
	rawKV := &model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      key,
		Value:    value,
		OldValue: nil,
		StartTs:  ts - 1,
		CRTs:     ts + 1,
	}
	polymorphicEvent := model.NewPolymorphicEvent(rawKV)
	err := tk.mounter.DecodeEvent(context.Background(), polymorphicEvent)
	require.NoError(tk.t, err)
	return polymorphicEvent.Row
}

func (tk *TestKit) DeleteDML2Event(deleteDML string, schema, table string) *model.RowChangedEvent {
	tk.MustExec(deleteDML)
	tableID, ok := tk.schemaStorage.GetLastSnapshot().TableIDByName(schema, table)
	require.True(tk.t, ok)
	key, value := tk.getLastKeyValue(tableID)

	ts := tk.schemaStorage.GetLastSnapshot().CurrentTs()
	rawKV := &model.RawKVEntry{
		OpType:   model.OpTypeDelete,
		Key:      key,
		Value:    value,
		OldValue: nil,
		StartTs:  ts - 1,
		CRTs:     ts,
	}
	polymorphicEvent := model.NewPolymorphicEvent(rawKV)
	err := tk.mounter.DecodeEvent(context.Background(), polymorphicEvent)
	require.NoError(tk.t, err)
	return polymorphicEvent.Row
}

func (tk *TestKit) getLastKeyValue(tableID int64) (key, value []byte) {
	txn, err := tk.storage.Begin()
	require.NoError(tk.t, err)
	defer txn.Rollback() //nolint:errcheck

	start, end := spanz.GetTableRange(tableID)
	iter, err := txn.Iter(start, end)
	require.NoError(tk.t, err)
	defer iter.Close()
	for iter.Valid() {
		key = iter.Key()
		value = iter.Value()
		err = iter.Next()
		require.NoError(tk.t, err)
	}
	return key, value
}

func (tk *TestKit) iterKeyValue(tableID int64, f func(key []byte, value []byte)) {
	txn, err := tk.storage.Begin()
	require.NoError(tk.t, err)
	defer txn.Rollback() //nolint:errcheck

	startKey, endKey := spanz.GetTableRange(tableID)
	iter, err := txn.Iter(startKey, endKey)
	require.NoError(tk.t, err)
	defer iter.Close()
	for iter.Valid() {
		f(iter.Key(), iter.Value())
		err = iter.Next()
		require.NoError(tk.t, err)
	}
}

// DDL2TableInfo executes the DDL stmt and returns the DDL job
func (tk *TestKit) DDL2TableInfo(ddl string) *model.TableInfo {
	tk.MustExec(ddl)
	jobs, err := tiddl.GetLastNHistoryDDLJobs(tk.GetCurrentMeta(), 1)
	require.NoError(tk.t, err)
	require.Len(tk.t, jobs, 1)
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
		require.NoError(tk.t, err)
		res.RawArgs = rawArgs
	}

	err = tk.schemaStorage.HandleDDLJob(res)
	require.NoError(tk.t, err)

	ver, err := tk.storage.CurrentVersion(oracle.GlobalTxnScope)
	require.NoError(tk.t, err)
	tk.schemaStorage.AdvanceResolvedTs(ver.Ver)

	tableInfo, ok := tk.schemaStorage.GetLastSnapshot().TableByName(res.SchemaName, res.TableName)
	require.True(tk.t, ok)

	return tableInfo
}

// DDL2Jobs executes the DDL statement and return the corresponding DDL jobs.
// It is mainly used for "DROP TABLE" and "DROP VIEW" statement because
// multiple jobs will be generated after executing these two types of
// DDL statements.
func (tk *TestKit) DDL2Jobs(ddl string, jobCnt int) []*timodel.Job {
	tk.MustExec(ddl)
	jobs, err := tiddl.GetLastNHistoryDDLJobs(tk.GetCurrentMeta(), jobCnt)
	require.Nil(tk.t, err)
	require.Len(tk.t, jobs, jobCnt)
	// Set State from Synced to Done.
	// Because jobs are put to history queue after TiDB alter its state from
	// Done to Synced.
	for i := range jobs {
		jobs[i].State = timodel.JobStateDone
	}
	return jobs
}

func (tk *TestKit) GetAllHistoryDDLJob() ([]*timodel.Job, error) {
	s, err := session.CreateSession(tk.storage)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if s != nil {
		defer s.Close()
	}

	store := domain.GetDomain(s.(sessionctx.Context)).Store()
	txn, err := store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer txn.Rollback() //nolint:errcheck
	txnMeta := timeta.NewMeta(txn)

	jobs, err := tiddl.GetAllHistoryDDLJobs(txnMeta)
	res := make([]*timodel.Job, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, job := range jobs {
		ignoreSchema := tk.filter.ShouldIgnoreSchema(job.SchemaName)
		ignoreTable := tk.filter.ShouldIgnoreTable(job.SchemaName, job.TableName)
		if ignoreSchema || ignoreTable {
			log.Info("Ignore ddl job", zap.Stringer("job", job))
			continue
		}
		// Set State from Synced to Done.
		// Because jobs are put to history queue after TiDB alter its state from
		// Done to Synced.
		jobs[i].State = timodel.JobStateDone
		res = append(res, job)
	}
	return jobs, nil
}

// Storage returns the tikv storage
func (tk *TestKit) Storage() kv.Storage {
	return tk.storage
}

// GetCurrentMeta return the current meta snapshot
func (tk *TestKit) GetCurrentMeta() *timeta.Meta {
	ver, err := tk.storage.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(tk.t, err)
	return timeta.NewSnapshotMeta(tk.storage.GetSnapshot(ver))
}

// Close closes the helper
func (tk *TestKit) Close() {
	tk.domain.Close()
	tk.storage.Close() //nolint:errcheck
}

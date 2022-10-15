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

package filter

import (
	"testing"

	ticonfig "github.com/pingcap/tidb/config"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

// testHelper is a test helper for filter which creates
// an internal tidb instance to generate DDL jobs with meta information
type testHelper struct {
	t       *testing.T
	tk      *testkit.TestKit
	storage kv.Storage
	domain  *domain.Domain
}

// newTestHelper creates a FilterTestHelper
func newTestHelper(t *testing.T) *testHelper {
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
	return &testHelper{
		t:       t,
		tk:      tk,
		storage: store,
		domain:  domain,
	}
}

// ddlToJob executes the DDL stmt and returns the DDL job
func (s *testHelper) ddlToJob(ddl string) *timodel.Job {
	s.tk.MustExec(ddl)
	jobs, err := tiddl.GetLastNHistoryDDLJobs(s.getCurrentMeta(), 1)
	require.Nil(s.t, err)
	require.Len(s.t, jobs, 1)
	return jobs[0]
}

// execDDL executes the DDL statement and returns the newest TableInfo of the table.
func (s *testHelper) execDDL(ddl string) *model.TableInfo {
	job := s.ddlToJob(ddl)
	ti, err := s.getCurrentMeta().GetTable(job.SchemaID, job.TableID)
	require.Nil(s.t, err)
	return model.WrapTableInfo(job.ID, job.SchemaName, job.BinlogInfo.FinishedTS, ti)
}

// getTk returns the TestKit
func (s *testHelper) getTk() *testkit.TestKit {
	return s.tk
}

// getCurrentMeta return the current meta snapshot
func (s *testHelper) getCurrentMeta() *timeta.Meta {
	ver, err := s.storage.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(s.t, err)
	return timeta.NewSnapshotMeta(s.storage.GetSnapshot(ver))
}

// close closes the helper
func (s *testHelper) close() {
	s.domain.Close()
	s.storage.Close() //nolint:errcheck
}

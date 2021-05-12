package entry

import (
	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/testkit"
)

type SchemaTestHelper struct {
	c       *check.C
	tk      *testkit.TestKit
	storage kv.Storage
	domain  *domain.Domain
}

func NewSchemaTestHelper(c *check.C) *SchemaTestHelper {
	store, err := mockstore.NewMockStore()
	c.Assert(err, check.IsNil)
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		conf.AlterPrimaryKey = true
	})
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	c.Assert(err, check.IsNil)
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(c, store)
	return &SchemaTestHelper{
		c:       c,
		tk:      tk,
		storage: store,
		domain:  domain,
	}
}

func (s *SchemaTestHelper) DDL2Job(ddl string) *timodel.Job {
	s.tk.MustExec(ddl)
	jobs, err := s.GetCurrentMeta().GetLastNHistoryDDLJobs(1)
	s.c.Assert(err, check.IsNil)
	s.c.Assert(jobs, check.HasLen, 1)
	return jobs[0]
}

func (s *SchemaTestHelper) Storage() kv.Storage {
	return s.storage
}

func (s *SchemaTestHelper) GetCurrentMeta() *timeta.Meta {
	ver, err := s.storage.CurrentVersion(oracle.GlobalTxnScope)
	s.c.Assert(err, check.IsNil)
	return timeta.NewSnapshotMeta(s.storage.GetSnapshot(ver))
}

func (s *SchemaTestHelper) Close() {
	s.domain.Close()
	s.storage.Close() //nolint:errcheck
}

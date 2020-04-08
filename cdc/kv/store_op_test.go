// Copyright 2019 PingCAP, Inc.
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

package kv

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
)

type storeSuite struct{}

var _ = check.Suite(&storeSuite{})

func (s *storeSuite) TestLoadHistoryDDLJobs(c *check.C) {
	mockCluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(mockCluster)
	mockMvccStore := mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(mockCluster),
		mockstore.WithMVCCStore(mockMvccStore),
	)
	c.Assert(err, check.IsNil)

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	c.Assert(err, check.IsNil)
	domain.SetStatsUpdating(true)

	oldJobIDs := make(map[int64]struct{})
	oldJobs, err := LoadHistoryDDLJobs(store)
	c.Assert(err, check.IsNil)
	for _, job := range oldJobs {
		oldJobIDs[job.ID] = struct{}{}
	}

	tk := testkit.NewTestKit(c, store)
	tk.MustExec("create table test.simple_test (id bigint primary key)")

	latestJobs, err := LoadHistoryDDLJobs(store)
	c.Assert(err, check.IsNil)
	c.Assert(len(latestJobs)-len(oldJobs), check.Equals, 1)
	_, ok := oldJobIDs[latestJobs[len(latestJobs)-1].ID]
	c.Assert(ok, check.IsFalse)
}

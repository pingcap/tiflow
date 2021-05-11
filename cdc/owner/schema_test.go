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

package owner

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"sort"
)

var _ = check.Suite(&schemaSuite{})

type schemaSuite struct {
}

func (s *schemaSuite) TestAllPhysicalTables(c *check.C) {
	helper := entry.NewSchemaTestHelper(c)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	c.Assert(err, check.IsNil)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver, config.GetDefaultReplicaConfig())
	c.Assert(err, check.IsNil)
	c.Assert(schema.AllPhysicalTables(), check.HasLen, 0)
	// add normal table
	job:=helper.DDL2Job("create table test.t1(id int primary key)")
	tableIDT1:=job.BinlogInfo.TableInfo.ID
	c.Assert(schema.HandleDDL(job), check.IsNil)
	c.Assert(schema.AllPhysicalTables(), check.DeepEquals, []model.TableID{tableIDT1})
	// add ineligible table
	c.Assert(schema.HandleDDL(helper.DDL2Job("create table test.t2(id int)")), check.IsNil)
	c.Assert(schema.AllPhysicalTables(), check.DeepEquals, []model.TableID{tableIDT1})
	// add partition table
	job=helper.DDL2Job(`CREATE TABLE test.employees  (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			fname VARCHAR(25) NOT NULL,
			lname VARCHAR(25) NOT NULL,
			store_id INT NOT NULL,
			department_id INT NOT NULL
		)

		PARTITION BY RANGE(id)  (
			PARTITION p0 VALUES LESS THAN (5),
			PARTITION p1 VALUES LESS THAN (10),
			PARTITION p2 VALUES LESS THAN (15),
			PARTITION p3 VALUES LESS THAN (20)
		)`)
	c.Assert(schema.HandleDDL(job), check.IsNil)
	expectedTableIDs:=[]model.TableID{tableIDT1}
	for _,p:=range job.BinlogInfo.TableInfo.GetPartitionInfo().Definitions{
		expectedTableIDs=append(expectedTableIDs,p.ID)
	}
	sortTableIDs:= func(tableIDs []model.TableID) {
		sort.Slice(tableIDs, func(i, j int) bool {
			return tableIDs[i]<tableIDs[j]
		})
	}
	sortTableIDs(expectedTableIDs)
	sortTableIDs(schema.AllPhysicalTables())
	c.Assert(schema.AllPhysicalTables(), check.DeepEquals, expectedTableIDs)
}

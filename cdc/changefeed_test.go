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

package cdc

import (
	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type changefeedSuite struct {
}

var _ = check.Suite(&changefeedSuite{})

func (s *changefeedSuite) TestUpdatePartition(c *check.C) {
	defer testleak.AfterTest(c)()

	cf := changeFeed{
		partitions: map[model.TableID][]int64{
			51: {53, 55, 57},
		},
		orphanTables:  make(map[model.TableID]model.Ts),
		toCleanTables: make(map[model.TableID]model.Ts),
	}
	tblInfo := &timodel.TableInfo{
		ID: 51,
		Partition: &timodel.PartitionInfo{
			Enable: true,
			Definitions: []timodel.PartitionDefinition{
				{ID: 57}, {ID: 59}, {ID: 61},
			},
		},
	}
	startTs := uint64(100)

	cf.updatePartition(tblInfo, startTs)
	c.Assert(cf.orphanTables, check.DeepEquals, map[model.TableID]model.Ts{
		59: startTs,
		61: startTs,
	})
	c.Assert(cf.toCleanTables, check.DeepEquals, map[model.TableID]model.Ts{
		53: startTs,
		55: startTs,
	})
	c.Assert(cf.partitions, check.DeepEquals, map[model.TableID][]int64{
		51: {57, 59, 61},
	})
}

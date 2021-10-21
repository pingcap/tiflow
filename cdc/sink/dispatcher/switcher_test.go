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

package dispatcher

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type SwitcherSuite struct{}

var _ = check.Suite(&SwitcherSuite{})

func (s SwitcherSuite) TestSwitcher(c *check.C) {
	defer testleak.AfterTest(c)()
	d, err := NewDispatcher(config.GetDefaultReplicaConfig(), 4)
	c.Assert(err, check.IsNil)
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test", Table: "test",
		},
	}), check.FitsTypeOf, &defaultDispatcher{})

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default.*"}, Dispatcher: "default"},
				{Matcher: []string{"test_table.*"}, Dispatcher: "table"},
				{Matcher: []string{"test_index_value.*"}, Dispatcher: "index-value"},
				{Matcher: []string{"test.*"}, Dispatcher: "rowid"},
				{Matcher: []string{"*.*", "!*.test"}, Dispatcher: "ts"},
				{Matcher: []string{"test_by_partition.*"}, Dispatcher: "3"},
				{Matcher: []string{"test_by_columns.*"}, Dispatcher: "[a, b]"},
			},
		},
	}, 4)
	c.Assert(err, check.IsNil)
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test", Table: "table1",
		},
	}), check.FitsTypeOf, &indexValueDispatcher{})
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "sbs", Table: "table2",
		},
	}), check.FitsTypeOf, &tsDispatcher{})
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "sbs", Table: "test",
		},
	}), check.FitsTypeOf, &defaultDispatcher{})
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test_default", Table: "test",
		},
	}), check.FitsTypeOf, &defaultDispatcher{})
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test_table", Table: "test",
		},
	}), check.FitsTypeOf, &tableDispatcher{})
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test_index_value", Table: "test",
		},
	}), check.FitsTypeOf, &indexValueDispatcher{})
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test_by_partition", Table: "test",
		},
	}), check.FitsTypeOf, &partitionIndexDispatcher{})
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test_by_columns", Table: "test",
		},
	}), check.FitsTypeOf, &columnsDispatcher{})
}

func (s SwitcherSuite) TestInvalidPartitionRule(c *check.C) {
	defer testleak.AfterTest(c)()
	invalidPartitionRule := []string{"-1", "3.14", "aloha", "[,]"}
	// for invalid partition number parameter, use `default`
	for _, n := range invalidPartitionRule {
		d, err := NewDispatcher(&config.ReplicaConfig{
			Sink: &config.SinkConfig{
				DispatchRules: []*config.DispatchRule{
					{Matcher: []string{"test_by_partition.*"}, Dispatcher: n},
				},
			},
		}, 4)
		c.Assert(err, check.IsNil)
		c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test_by_partition", Table: "test",
			},
		}), check.FitsTypeOf, &defaultDispatcher{})
	}
}

func (s SwitcherSuite) TestByPartitionDispatcher(c *check.C) {
	defer testleak.AfterTest(c)()
	d, err := NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_by_partition.*"}, Partition: "4"}, // equal to partitionNum, out of index.
			},
		},
	}, 4)
	c.Assert(err, check.ErrorMatches, ".*can't create partition dispatcher.*")
	c.Assert(d, check.IsNil)

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_by_partition.*"}, Partition: "-1"}, // invalid target partition index
			},
		},
	}, 4)
	c.Assert(err, check.ErrorMatches, ".*can't create partition dispatcher.*")
	c.Assert(d, check.IsNil)

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_by_partition.*"}, Partition: "2"}, // equal to partitionNum, out of index.
			},
		},
	}, 4)
	c.Assert(err, check.IsNil)
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test_by_partition", Table: "test",
		},
	}), check.FitsTypeOf, &partitionIndexDispatcher{})
}

func (s SwitcherSuite) TestByColumnDispatcher(c *check.C) {
	defer testleak.AfterTest(c)()
	d, err := NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_by_columns.*"}, Dispatcher: "[]"}, // equal to partitionNum, out of index.
			},
		},
	}, 4)
	c.Assert(err, check.IsNil)

	row := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test_by_columns", Table: "test",
		},
	}
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(row), check.FitsTypeOf, &columnsDispatcher{})
	c.Assert(d.Dispatch(row), check.Equals, int32(0))

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_by_columns.*"}, Dispatcher: "[a, b]"},
			},
		},
	}, 4)
	c.Assert(err, check.IsNil)

	row = &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test_by_columns", Table: "test",
		},
		Columns: []*model.Column{
			{
				Name:  "a",
				Value: 1,
			},
			{
				Name:  "b",
				Value: 2,
			},
		},
	}
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(row), check.FitsTypeOf, &columnsDispatcher{})
	c.Assert(d.Dispatch(row), check.Not(check.Equals), int32(0))
}

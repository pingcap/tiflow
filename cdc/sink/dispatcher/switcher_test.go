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
)

type SwitcherSuite struct{}

var _ = check.Suite(&SwitcherSuite{})

func (s SwitcherSuite) TestSwitcher(c *check.C) {
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
				{Matcher: []string{"test.*"}, Dispatcher: "rowid"},
				{Matcher: []string{"*.*", "!*.test"}, Dispatcher: "ts"},
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
}

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
				{Matcher: []string{"test.*"}, Rules: "rowid"},
				{Matcher: []string{"*.*", "!*.test"}, Rules: "ts"},
			},
		},
	}, 4)
	c.Assert(err, check.IsNil)
	c.Assert(d.(*dispatcherSwitcher).matchDispatcher(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test", Table: "table1",
		},
	}), check.FitsTypeOf, &rowIDDispatcher{})
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

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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestDefaultSwitcher(t *testing.T) {
	t.Parallel()

	d, err := NewDispatcher(config.GetDefaultReplicaConfig(), 4, model.SinkTypeUnknown)
	require.Nil(t, d)
	require.Error(t, err)

	d, err = NewDispatcher(config.GetDefaultReplicaConfig(), 4, model.SinkTypeMQ)
	require.Nil(t, err)
	require.IsType(t, &defaultDispatcher{}, d.(*dispatcherSwitcher).matchDispatcher(&model.RawTableTxn{
		Table: &model.TableName{
			Schema: "test", Table: "test",
		},
	}))

	d, err = NewDispatcher(config.GetDefaultReplicaConfig(), 4, model.SinkTypeMySQL)
	require.Nil(t, err)
	require.IsType(t, &causalityDispatcher{}, d.(*dispatcherSwitcher).matchDispatcher(&model.RawTableTxn{
		Table: &model.TableName{
			Schema: "test", Table: "test",
		},
	}))
}

func TestSupportedSwitcher(t *testing.T) {
	t.Parallel()

	d, err := NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default.*"}, Dispatcher: "default"},
				{Matcher: []string{"test_table.*"}, Dispatcher: "table"},
				{Matcher: []string{"test_index_value.*"}, Dispatcher: "index-value"},
				{Matcher: []string{"test.*"}, Dispatcher: "rowid"},
				{Matcher: []string{"*.*", "!*.test"}, Dispatcher: "ts"},
			},
		},
	}, 4, model.SinkTypeMQ)
	require.Nil(t, err)
	require.NotNil(t, d)

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test.*"}, Dispatcher: "causality"},
			},
		},
	}, 4, model.SinkTypeMQ)
	require.Error(t, err)
	require.NotNil(t, d)

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default.*"}, Dispatcher: "table"},
				{Matcher: []string{"test.*"}, Dispatcher: "causality"},
			},
		},
	}, 4, model.SinkTypeMySQL)
	require.Nil(t, err)
	require.NotNil(t, d)

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default.*"}, Dispatcher: "default"},
			},
		},
	}, 4, model.SinkTypeMySQL)
	require.Error(t, err)
	require.NotNil(t, d)

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default.*"}, Dispatcher: "index-value"},
			},
		},
	}, 4, model.SinkTypeMySQL)
	require.Error(t, err)
	require.NotNil(t, d)

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default.*"}, Dispatcher: "ts"},
			},
		},
	}, 4, model.SinkTypeMySQL)
	require.Error(t, err)
	require.NotNil(t, d)

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default.*"}, Dispatcher: "rowid"},
			},
		},
	}, 4, model.SinkTypeMySQL)
	require.Error(t, err)
	require.NotNil(t, d)
}

func TestMQSwitcher(t *testing.T) {
	d, err := NewDispatcher(config.GetDefaultReplicaConfig(), 4, model.SinkTypeMQ)
	require.Nil(t, err)
	require.IsType(t, &defaultDispatcher{}, d.(*dispatcherSwitcher).matchDispatcher(&model.RawTableTxn{
		Table: &model.TableName{
			Schema: "test", Table: "test",
		},
	}))

	d, err = NewDispatcher(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default.*"}, Dispatcher: "default"},
				{Matcher: []string{"test_table.*"}, Dispatcher: "table"},
				{Matcher: []string{"test_index_value.*"}, Dispatcher: "index-value"},
				{Matcher: []string{"test.*"}, Dispatcher: "rowid"},
				{Matcher: []string{"*.*", "!*.test"}, Dispatcher: "ts"},
			},
		},
	}, 4, model.SinkTypeMQ)
	require.Nil(t, err)
	require.IsType(t, &indexValueDispatcher{}, d.(*dispatcherSwitcher).matchDispatcher(&model.RawTableTxn{
		Table: &model.TableName{
			Schema: "test", Table: "table1",
		},
	}))
	require.IsType(t, &tsDispatcher{}, d.(*dispatcherSwitcher).matchDispatcher(&model.RawTableTxn{
		Table: &model.TableName{
			Schema: "sbs", Table: "table2",
		},
	}))
	require.IsType(t, &defaultDispatcher{}, d.(*dispatcherSwitcher).matchDispatcher(&model.RawTableTxn{
		Table: &model.TableName{
			Schema: "sbs", Table: "test",
		},
	}))
	require.IsType(t, &defaultDispatcher{}, d.(*dispatcherSwitcher).matchDispatcher(&model.RawTableTxn{
		Table: &model.TableName{
			Schema: "test_default", Table: "test",
		},
	}))
	require.IsType(t, &tableDispatcher{}, d.(*dispatcherSwitcher).matchDispatcher(&model.RawTableTxn{
		Table: &model.TableName{
			Schema: "test_table", Table: "test",
		},
	}))
	require.IsType(t, &indexValueDispatcher{}, d.(*dispatcherSwitcher).matchDispatcher(&model.RawTableTxn{
		Table: &model.TableName{
			Schema: "test_index_value", Table: "test",
		},
	}))
}

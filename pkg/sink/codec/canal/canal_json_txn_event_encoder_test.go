// Copyright 2023 PingCAP, Inc.
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

package canal

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestBuildCanalJSONTxnEventEncoder(t *testing.T) {
	t.Parallel()
	cfg := common.NewConfig(config.ProtocolCanalJSON)

	builder := NewJSONTxnEventEncoderBuilder(cfg)
	encoder, ok := builder.Build().(*JSONTxnEventEncoder)
	require.True(t, ok)
	require.False(t, encoder.enableTiDBExtension)

	cfg.EnableTiDBExtension = true
	builder = NewJSONTxnEventEncoderBuilder(cfg)
	encoder, ok = builder.Build().(*JSONTxnEventEncoder)
	require.True(t, ok)
	require.True(t, encoder.enableTiDBExtension)
}

func TestCanalJSONTxnEventEncoderMaxMessageBytes(t *testing.T) {
	t.Parallel()

	// the size of `testEvent` after being encoded by canal-json is 200
	testEvent := &model.SingleTableTxn{
		Table: &model.TableName{Schema: "a", Table: "b"},
		Rows: []*model.RowChangedEvent{
			{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns: []*model.Column{{
					Name:  "col1",
					Type:  mysql.TypeVarchar,
					Value: []byte("aa"),
				}},
			},
		},
	}

	// the test message length is smaller than max-message-bytes
	maxMessageBytes := 300
	cfg := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(maxMessageBytes)
	encoder := NewJSONTxnEventEncoderBuilder(cfg).Build()
	err := encoder.AppendTxnEvent(testEvent, nil)
	require.Nil(t, err)

	// the test message length is larger than max-message-bytes
	cfg = cfg.WithMaxMessageBytes(100)
	encoder = NewJSONTxnEventEncoderBuilder(cfg).Build()
	err = encoder.AppendTxnEvent(testEvent, nil)
	require.NotNil(t, err)
}

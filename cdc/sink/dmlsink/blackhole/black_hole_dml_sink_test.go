// Copyright 2022 PingCAP, Inc.
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

package blackhole

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/stretchr/testify/require"
)

func TestWriteEventsCallback(t *testing.T) {
	t.Parallel()

	s := NewDMLSink()
	tableStatus := state.TableSinkSinking
	row := &model.RowChangedEvent{
		CommitTs: 1,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "a",
				Table:  "b",
			},
		},
		Columns: []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	count := 0
	events := make([]*dmlsink.RowChangeCallbackableEvent, 0, 3000)
	for i := 0; i < 3000; i++ {
		events = append(events, &dmlsink.RowChangeCallbackableEvent{
			Event: row,
			Callback: func() {
				count++
			},
			SinkState: &tableStatus,
		})
	}
	err := s.WriteEvents(events...)
	require.Nil(t, err, "no error should be returned")
	require.Equal(t, 3000, count, "all callbacks should be called")
}

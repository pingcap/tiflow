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

package topic

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestStaticTopicDispatcher(t *testing.T) {
	row := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "db1",
			Table:  "tbl1",
		},
	}

	ddl := &model.DDLEvent{
		TableInfo: &model.SimpleTableInfo{
			Schema: "db1",
			Table:  "tbl1",
		},
	}

	p := NewStaticTopicDispatcher("cdctest")
	require.Equal(t, p.DispatchRowChangedEvent(row), "cdctest")
	require.Equal(t, p.DispatchDDLEvent(ddl), "cdctest")
}

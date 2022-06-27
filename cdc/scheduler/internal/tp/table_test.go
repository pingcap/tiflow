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

package tp

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/stretchr/testify/require"
)

func TestTableManager(t *testing.T) {
	t.Parallel()

	// pretend there are 4 tables
	mockTableExecutor := newMockTableExecutor()

	tableM := newTableManager(mockTableExecutor)

	tableM.addTable(model.TableID(1))
	require.Equal(t, schedulepb.TableStateAbsent, tableM.tables[model.TableID(1)].state)

	tableM.dropTable(model.TableID(1))
	require.NotContains(t, tableM.tables, model.TableID(1))
}

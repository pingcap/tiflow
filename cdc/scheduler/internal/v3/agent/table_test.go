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

package agent

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestTableManager(t *testing.T) {
	t.Parallel()

	// pretend there are 4 tables
	mockTableExecutor := newMockTableExecutor()

	tableM := newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)

	span1 := spanz.TableIDToComparableSpan(1)
	tableM.addTableSpan(span1)
	require.Equal(t, tablepb.TableStateAbsent, tableM.tables.GetV(span1).state)

	tableM.dropTableSpan(span1)
	require.False(t, tableM.tables.Has(span1))
}

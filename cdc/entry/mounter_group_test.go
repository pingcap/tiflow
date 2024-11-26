// Copyright 2024 PingCAP, Inc.
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

package entry

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestWorkerNum(t *testing.T) {
	mg := NewMounterGroup(nil, -1, nil, nil, model.ChangeFeedID4Test("", ""), nil)
	require.Equal(t, mg.workerNum, defaultMounterWorkerNum)
	mg = NewMounterGroup(nil, maxMounterWorkerNum+10, nil, nil, model.ChangeFeedID4Test("", ""), nil)
	require.Equal(t, mg.workerNum, maxMounterWorkerNum)
}

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

package runtime

import (
	"testing"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/stretchr/testify/require"
)

func TestTaskStatus(t *testing.T) {
	t.Parallel()

	offlineStatus := NewOfflineStatus("task_status_test")
	require.Equal(t, offlineStatus.Unit, frameModel.WorkerType(0))
	require.Equal(t, offlineStatus.Task, "task_status_test")
	require.Equal(t, offlineStatus.Stage, metadata.StageUnscheduled)
	require.Equal(t, offlineStatus.CfgModRevision, uint64(0))
}

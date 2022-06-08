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

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
)

func TestTaskStatus(t *testing.T) {
	t.Parallel()

	offlineStatus := NewOfflineStatus("task_status_test")
	require.Equal(t, offlineStatus.Unit, libModel.WorkerType(0))
	require.Equal(t, offlineStatus.Task, "task_status_test")
	require.Equal(t, offlineStatus.Stage, metadata.StageUnscheduled)
}

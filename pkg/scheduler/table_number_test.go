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

package scheduler

import (
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/cdc/model"

	"github.com/stretchr/testify/require"
)

func TestDistributeTables(t *testing.T) {
	t.Parallel()
	scheduler := newTableNumberScheduler()
	scheduler.ResetWorkloads("capture1", model.TaskWorkload{
		1: model.WorkloadInfo{Workload: 1},
		2: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture2", model.TaskWorkload{
		3: model.WorkloadInfo{Workload: 1},
		4: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture3", model.TaskWorkload{
		5: model.WorkloadInfo{Workload: 1},
		6: model.WorkloadInfo{Workload: 1},
		7: model.WorkloadInfo{Workload: 1},
		8: model.WorkloadInfo{Workload: 1},
	})
	require.Equal(t, fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), "35.36%")
	tableToAdd := map[model.TableID]model.Ts{10: 1, 11: 2, 12: 3, 13: 4, 14: 5, 15: 6, 16: 7, 17: 8}
	result := scheduler.DistributeTables(tableToAdd)
	require.Equal(t, len(result), 3)
	totalTableNum := 0
	for _, ops := range result {
		for tableID, op := range ops {
			ts, exist := tableToAdd[tableID]
			require.True(t, exist)
			require.False(t, op.Delete)
			require.Equal(t, op.BoundaryTs, ts)
			totalTableNum++
		}
	}
	require.Equal(t, totalTableNum, 8)
	require.Equal(t, fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), "8.84%")
}

func TestCalRebalanceOperates(t *testing.T) {
	t.Parallel()
	scheduler := newTableNumberScheduler()
	scheduler.ResetWorkloads("capture1", model.TaskWorkload{
		1: model.WorkloadInfo{Workload: 1},
		2: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture2", model.TaskWorkload{
		3: model.WorkloadInfo{Workload: 1},
		4: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture3", model.TaskWorkload{
		5:  model.WorkloadInfo{Workload: 1},
		6:  model.WorkloadInfo{Workload: 1},
		7:  model.WorkloadInfo{Workload: 1},
		8:  model.WorkloadInfo{Workload: 1},
		9:  model.WorkloadInfo{Workload: 1},
		10: model.WorkloadInfo{Workload: 1},
	})
	require.Equal(t, fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), "56.57%")
	skewness, moveJobs := scheduler.CalRebalanceOperates(0)

	for tableID, job := range moveJobs {
		require.Greater(t, len(job.From), 0)
		require.Greater(t, len(job.To), 0)
		require.Equal(t, job.TableID, tableID)
		require.NotEqual(t, job.From, job.To)
		require.Equal(t, job.Status, model.MoveTableStatusNone)
	}

	require.Equal(t, fmt.Sprintf("%.2f%%", skewness*100), "14.14%")

	scheduler.ResetWorkloads("capture1", model.TaskWorkload{
		1: model.WorkloadInfo{Workload: 1},
		2: model.WorkloadInfo{Workload: 1},
		3: model.WorkloadInfo{Workload: 1},
	})
	scheduler.ResetWorkloads("capture2", model.TaskWorkload{})
	scheduler.ResetWorkloads("capture3", model.TaskWorkload{})
	require.Equal(t, fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), "141.42%")
	skewness, moveJobs = scheduler.CalRebalanceOperates(0)

	for tableID, job := range moveJobs {
		require.Greater(t, len(job.From), 0)
		require.Greater(t, len(job.To), 0)
		require.Equal(t, job.TableID, tableID)
		require.NotEqual(t, job.From, job.To)
		require.Equal(t, job.Status, model.MoveTableStatusNone)
	}
	require.Equal(t, fmt.Sprintf("%.2f%%", skewness*100), "0.00%")
}

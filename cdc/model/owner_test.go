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

package model

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/stretchr/testify/require"
)

func TestAdminJobType(t *testing.T) {
	t.Parallel()

	names := map[AdminJobType]string{
		AdminNone:         "noop",
		AdminStop:         "stop changefeed",
		AdminResume:       "resume changefeed",
		AdminRemove:       "remove changefeed",
		AdminFinish:       "finish changefeed",
		AdminJobType(100): "unknown",
	}
	for job, name := range names {
		require.Equal(t, name, job.String())
	}

	isStopped := map[AdminJobType]bool{
		AdminNone:   false,
		AdminStop:   true,
		AdminResume: false,
		AdminRemove: true,
		AdminFinish: true,
	}
	for job, stopped := range isStopped {
		require.Equal(t, stopped, job.IsStopState())
	}
}

func TestTaskPositionMarshal(t *testing.T) {
	t.Parallel()

	pos := &TaskPosition{
		ResolvedTs:   420875942036766723,
		CheckPointTs: 420875940070686721,
	}
	expected := `{"checkpoint-ts":420875940070686721,"resolved-ts":420875942036766723,"count":0,"error":null}`

	data, err := pos.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, data)
	require.Equal(t, expected, pos.String())

	newPos := &TaskPosition{}
	err = newPos.Unmarshal([]byte(data))
	require.Nil(t, err)
	require.Equal(t, pos, newPos)
}

func TestChangeFeedStatusMarshal(t *testing.T) {
	t.Parallel()

	status := &ChangeFeedStatus{
		ResolvedTs:   420875942036766723,
		CheckpointTs: 420875940070686721,
	}
	expected := `{"resolved-ts":420875942036766723,"checkpoint-ts":420875940070686721,"admin-job-type":0}`

	data, err := status.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, data)

	newStatus := &ChangeFeedStatus{}
	err = newStatus.Unmarshal([]byte(data))
	require.Nil(t, err)
	require.Equal(t, status, newStatus)
}

func TestTableOperationState(t *testing.T) {
	t.Parallel()

	processedMap := map[uint64]bool{
		OperDispatched: false,
		OperProcessed:  true,
		OperFinished:   true,
	}
	appliedMap := map[uint64]bool{
		OperDispatched: false,
		OperProcessed:  false,
		OperFinished:   true,
	}
	o := &TableOperation{}

	for status, processed := range processedMap {
		o.Status = status
		require.Equal(t, processed, o.TableProcessed())
	}
	for status, applied := range appliedMap {
		o.Status = status
		require.Equal(t, applied, o.TableApplied())
	}

	// test clone nil operation. no-nil clone will be tested in `TestShouldBeDeepCopy`
	var nilTableOper *TableOperation
	require.Nil(t, nilTableOper.Clone())
}

func TestTaskWorkloadMarshal(t *testing.T) {
	t.Parallel()

	workload := &TaskWorkload{
		12: WorkloadInfo{Workload: uint64(1)},
		15: WorkloadInfo{Workload: uint64(3)},
	}
	expected := `{"12":{"workload":1},"15":{"workload":3}}`

	data, err := workload.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, data)

	newWorkload := &TaskWorkload{}
	err = newWorkload.Unmarshal([]byte(data))
	require.Nil(t, err)
	require.Equal(t, workload, newWorkload)

	workload = nil
	data, err = workload.Marshal()
	require.Nil(t, err)
	require.Equal(t, "{}", data)
}

type taskStatusSuite struct{}

var _ = check.Suite(&taskStatusSuite{})

func TestShouldBeDeepCopy(t *testing.T) {
	t.Parallel()

	info := TaskStatus{
		Tables: map[TableID]*TableReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 100},
			3: {StartTs: 100},
			4: {StartTs: 100},
		},
		Operation: map[TableID]*TableOperation{
			5: {
				Delete: true, BoundaryTs: 6,
			},
			6: {
				Delete: false, BoundaryTs: 7,
			},
		},
		AdminJobType: AdminStop,
	}

	clone := info.Clone()
	assertIsSnapshot := func() {
		require.Equal(t, map[TableID]*TableReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 100},
			3: {StartTs: 100},
			4: {StartTs: 100},
		}, clone.Tables)
		require.Equal(t, map[TableID]*TableOperation{
			5: {
				Delete: true, BoundaryTs: 6,
			},
			6: {
				Delete: false, BoundaryTs: 7,
			},
		}, clone.Operation)
		require.Equal(t, AdminStop, clone.AdminJobType)
	}

	assertIsSnapshot()

	info.Tables[7] = &TableReplicaInfo{StartTs: 100}
	info.Operation[7] = &TableOperation{Delete: true, BoundaryTs: 7}

	info.Operation[5].BoundaryTs = 8
	info.Tables[1].StartTs = 200

	assertIsSnapshot()
}

func TestTaskStatusMarshal(t *testing.T) {
	t.Parallel()

	status := &TaskStatus{
		Tables: map[TableID]*TableReplicaInfo{
			1: {StartTs: 420875942036766723},
		},
	}
	expected := `{"tables":{"1":{"start-ts":420875942036766723,"mark-table-id":0}},"operation":null,"admin-job-type":0}`

	data, err := status.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, data)
	require.Equal(t, expected, status.String())

	newStatus := &TaskStatus{}
	err = newStatus.Unmarshal([]byte(data))
	require.Nil(t, err)
	require.Equal(t, status, newStatus)
}

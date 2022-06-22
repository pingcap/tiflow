// Copyright 2021 PingCAP, Inc.
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

package orchestrator

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator/util"
	"github.com/stretchr/testify/require"
)

func TestCheckCaptureAlive(t *testing.T) {
	state := NewChangefeedReactorState("test")
	stateTester := NewReactorStateTester(t, state, nil)
	state.CheckCaptureAlive("6bbc01c8-0605-4f86-a0f9-b3119109b225")
	require.Contains(t, stateTester.ApplyPatches().Error(), "[CDC:ErrLeaseExpired]")
	err := stateTester.Update("/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225", []byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`))
	require.Nil(t, err)
	state.CheckCaptureAlive("6bbc01c8-0605-4f86-a0f9-b3119109b225")
	stateTester.MustApplyPatches()
}

func TestChangefeedStateUpdate(t *testing.T) {
	createTime, err := time.Parse("2006-01-02", "2020-02-02")
	require.Nil(t, err)
	testCases := []struct {
		changefeedID string
		updateKey    []string
		updateValue  []string
		expected     ChangefeedReactorState
	}{
		{ // common case
			changefeedID: "test1",
			updateKey: []string{
				"/tidb/cdc/changefeed/info/test1",
				"/tidb/cdc/job/test1",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
			},
			updateValue: []string{
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":"","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"open-protocol"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1},"consistent":{"level":"normal","storage":"local"}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"tables":{"45":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
			},
			expected: ChangefeedReactorState{
				ID: "test1",
				Info: &model.ChangeFeedInfo{
					SinkURI:           "blackhole://",
					Opts:              map[string]string{},
					CreateTime:        createTime,
					StartTs:           421980685886554116,
					Engine:            model.SortInMemory,
					State:             "normal",
					SyncPointInterval: time.Minute * 10,
					Config: &config.ReplicaConfig{
						CaseSensitive:    true,
						CheckGCSafePoint: true,
						Filter:           &config.FilterConfig{Rules: []string{"*.*"}},
						Mounter:          &config.MounterConfig{WorkerNum: 16},
						Sink:             &config.SinkConfig{Protocol: "open-protocol"},
						Cyclic:           &config.CyclicConfig{},
						Scheduler:        &config.SchedulerConfig{Tp: "table-number", PollingTime: -1},
						Consistent:       &config.ConsistentConfig{Level: "normal", Storage: "local"},
					},
				},
				Status: &model.ChangeFeedStatus{CheckpointTs: 421980719742451713, ResolvedTs: 421980720003809281},
				TaskStatuses: map[model.CaptureID]*model.TaskStatus{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
						Tables: map[int64]*model.TableReplicaInfo{45: {StartTs: 421980685886554116}},
					},
				},
				TaskPositions: map[model.CaptureID]*model.TaskPosition{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
				},
				Workloads: map[model.CaptureID]model.TaskWorkload{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {45: {Workload: 1}},
				},
			},
		},
		{ // test multiple capture
			changefeedID: "test1",
			updateKey: []string{
				"/tidb/cdc/changefeed/info/test1",
				"/tidb/cdc/job/test1",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				"/tidb/cdc/task/position/666777888/test1",
				"/tidb/cdc/task/status/666777888/test1",
				"/tidb/cdc/task/workload/666777888/test1",
				"/tidb/cdc/capture/666777888",
			},
			updateValue: []string{
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":"","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"open-protocol"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1},"consistent":{"level":"normal","storage":"local"}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"tables":{"45":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`{"checkpoint-ts":11332244,"resolved-ts":312321,"count":8,"error":null}`,
				`{"tables":{"46":{"start-ts":412341234,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"46":{"workload":3}}`,
				`{"id":"666777888","address":"127.0.0.1:8300"}`,
			},
			expected: ChangefeedReactorState{
				ID: "test1",
				Info: &model.ChangeFeedInfo{
					SinkURI:           "blackhole://",
					Opts:              map[string]string{},
					CreateTime:        createTime,
					StartTs:           421980685886554116,
					Engine:            model.SortInMemory,
					State:             "normal",
					SyncPointInterval: time.Minute * 10,
					Config: &config.ReplicaConfig{
						CaseSensitive:    true,
						CheckGCSafePoint: true,
						Filter:           &config.FilterConfig{Rules: []string{"*.*"}},
						Mounter:          &config.MounterConfig{WorkerNum: 16},
						Sink:             &config.SinkConfig{Protocol: "open-protocol"},
						Cyclic:           &config.CyclicConfig{},
						Scheduler:        &config.SchedulerConfig{Tp: "table-number", PollingTime: -1},
						Consistent:       &config.ConsistentConfig{Level: "normal", Storage: "local"},
					},
				},
				Status: &model.ChangeFeedStatus{CheckpointTs: 421980719742451713, ResolvedTs: 421980720003809281},
				TaskStatuses: map[model.CaptureID]*model.TaskStatus{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
						Tables: map[int64]*model.TableReplicaInfo{45: {StartTs: 421980685886554116}},
					},
					"666777888": {
						Tables: map[int64]*model.TableReplicaInfo{46: {StartTs: 412341234}},
					},
				},
				TaskPositions: map[model.CaptureID]*model.TaskPosition{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
					"666777888":                            {CheckPointTs: 11332244, ResolvedTs: 312321, Count: 8},
				},
				Workloads: map[model.CaptureID]model.TaskWorkload{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {45: {Workload: 1}},
					"666777888":                            {46: {Workload: 3}},
				},
			},
		},
		{ // testing changefeedID not match
			changefeedID: "test1",
			updateKey: []string{
				"/tidb/cdc/changefeed/info/test1",
				"/tidb/cdc/job/test1",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				"/tidb/cdc/changefeed/info/test-fake",
				"/tidb/cdc/job/test-fake",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test-fake",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test-fake",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test-fake",
			},
			updateValue: []string{
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":"","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"open-protocol"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1},"consistent":{"level":"normal","storage":"local"}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"tables":{"45":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`fake value`,
				`fake value`,
				`fake value`,
				`fake value`,
				`fake value`,
			},
			expected: ChangefeedReactorState{
				ID: "test1",
				Info: &model.ChangeFeedInfo{
					SinkURI:           "blackhole://",
					Opts:              map[string]string{},
					CreateTime:        createTime,
					StartTs:           421980685886554116,
					Engine:            model.SortInMemory,
					State:             "normal",
					SyncPointInterval: time.Minute * 10,
					Config: &config.ReplicaConfig{
						CaseSensitive:    true,
						CheckGCSafePoint: true,
						Filter:           &config.FilterConfig{Rules: []string{"*.*"}},
						Mounter:          &config.MounterConfig{WorkerNum: 16},
						Sink:             &config.SinkConfig{Protocol: "open-protocol"},
						Cyclic:           &config.CyclicConfig{},
						Scheduler:        &config.SchedulerConfig{Tp: "table-number", PollingTime: -1},
						Consistent:       &config.ConsistentConfig{Level: "normal", Storage: "local"},
					},
				},
				Status: &model.ChangeFeedStatus{CheckpointTs: 421980719742451713, ResolvedTs: 421980720003809281},
				TaskStatuses: map[model.CaptureID]*model.TaskStatus{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
						Tables: map[int64]*model.TableReplicaInfo{45: {StartTs: 421980685886554116}},
					},
				},
				TaskPositions: map[model.CaptureID]*model.TaskPosition{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
				},
				Workloads: map[model.CaptureID]model.TaskWorkload{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {45: {Workload: 1}},
				},
			},
		},
		{ // testing value is nil
			changefeedID: "test1",
			updateKey: []string{
				"/tidb/cdc/changefeed/info/test1",
				"/tidb/cdc/job/test1",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				"/tidb/cdc/task/position/666777888/test1",
				"/tidb/cdc/task/status/666777888/test1",
				"/tidb/cdc/task/workload/666777888/test1",
				"/tidb/cdc/changefeed/info/test1",
				"/tidb/cdc/job/test1",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				"/tidb/cdc/task/workload/666777888/test1",
				"/tidb/cdc/task/status/666777888/test1",
			},
			updateValue: []string{
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":"","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"open-protocol"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1},"consistent":{"level":"normal","storage":"local"}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"tables":{"45":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`{"checkpoint-ts":11332244,"resolved-ts":312321,"count":8,"error":null}`,
				`{"tables":{"46":{"start-ts":412341234,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"46":{"workload":3}}`,
				``,
				``,
				``,
				``,
				``,
				``,
				``,
				``,
			},
			expected: ChangefeedReactorState{
				ID:           "test1",
				Info:         nil,
				Status:       nil,
				TaskStatuses: map[model.CaptureID]*model.TaskStatus{},
				TaskPositions: map[model.CaptureID]*model.TaskPosition{
					"666777888": {CheckPointTs: 11332244, ResolvedTs: 312321, Count: 8},
				},
				Workloads: map[model.CaptureID]model.TaskWorkload{},
			},
		},
		{ // testing the same key case
			changefeedID: "test1",
			updateKey: []string{
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
			},
			updateValue: []string{
				`{"tables":{"45":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"tables":{"46":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				``,
				`{"tables":{"47":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
			},
			expected: ChangefeedReactorState{
				ID: "test1",
				TaskStatuses: map[model.CaptureID]*model.TaskStatus{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
						Tables: map[int64]*model.TableReplicaInfo{47: {StartTs: 421980685886554116}},
					},
				},
				TaskPositions: map[model.CaptureID]*model.TaskPosition{},
				Workloads:     map[model.CaptureID]model.TaskWorkload{},
			},
		},
	}
	for i, tc := range testCases {
		state := NewChangefeedReactorState(tc.changefeedID)
		for i, k := range tc.updateKey {
			value := []byte(tc.updateValue[i])
			if len(value) == 0 {
				value = nil
			}
			err = state.Update(util.NewEtcdKey(k), value, false)
			require.Nil(t, err)
		}
		require.True(t, cmp.Equal(state, &tc.expected, cmpopts.IgnoreUnexported(ChangefeedReactorState{})),
			fmt.Sprintf("%d,%s", i, cmp.Diff(state, &tc.expected, cmpopts.IgnoreUnexported(ChangefeedReactorState{}))))
	}
}

func TestPatchInfo(t *testing.T) {
	state := NewChangefeedReactorState("test1")
	stateTester := NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	stateTester.MustApplyPatches()
	defaultConfig := config.GetDefaultReplicaConfig()
	require.Equal(t, state.Info, &model.ChangeFeedInfo{
		SinkURI: "123",
		Engine:  model.SortUnified,
		Config: &config.ReplicaConfig{
			Filter:     defaultConfig.Filter,
			Mounter:    defaultConfig.Mounter,
			Sink:       defaultConfig.Sink,
			Cyclic:     defaultConfig.Cyclic,
			Scheduler:  defaultConfig.Scheduler,
			Consistent: defaultConfig.Consistent,
		},
	})
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		info.StartTs = 6
		return info, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.Info, &model.ChangeFeedInfo{
		SinkURI: "123",
		StartTs: 6,
		Engine:  model.SortUnified,
		Config: &config.ReplicaConfig{
			Filter:     defaultConfig.Filter,
			Mounter:    defaultConfig.Mounter,
			Sink:       defaultConfig.Sink,
			Cyclic:     defaultConfig.Cyclic,
			Scheduler:  defaultConfig.Scheduler,
			Consistent: defaultConfig.Consistent,
		},
	})
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	require.Nil(t, state.Info)
}

func TestPatchStatus(t *testing.T) {
	state := NewChangefeedReactorState("test1")
	stateTester := NewReactorStateTester(t, state, nil)
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{CheckpointTs: 5}, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status, &model.ChangeFeedStatus{CheckpointTs: 5})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.ResolvedTs = 6
		return status, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status, &model.ChangeFeedStatus{CheckpointTs: 5, ResolvedTs: 6})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	require.Nil(t, state.Status)
}

func TestPatchTaskPosition(t *testing.T) {
	state := NewChangefeedReactorState("test1")
	stateTester := NewReactorStateTester(t, state, nil)
	captureID1 := "capture1"
	captureID2 := "capture2"
	state.PatchTaskPosition(captureID1, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		require.Nil(t, position)
		return &model.TaskPosition{
			CheckPointTs: 1,
		}, true, nil
	})
	state.PatchTaskPosition(captureID2, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		require.Nil(t, position)
		return &model.TaskPosition{
			CheckPointTs: 2,
		}, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.TaskPositions, map[string]*model.TaskPosition{
		captureID1: {
			CheckPointTs: 1,
		},
		captureID2: {
			CheckPointTs: 2,
		},
	})
	state.PatchTaskPosition(captureID1, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		position.CheckPointTs = 3
		return position, true, nil
	})
	state.PatchTaskPosition(captureID2, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		position.ResolvedTs = 2
		return position, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.TaskPositions, map[string]*model.TaskPosition{
		captureID1: {
			CheckPointTs: 3,
		},
		captureID2: {
			CheckPointTs: 2,
			ResolvedTs:   2,
		},
	})
	state.PatchTaskPosition(captureID1, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return nil, false, nil
	})
	state.PatchTaskPosition(captureID2, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return nil, true, nil
	})
	state.PatchTaskPosition(captureID1, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		position.Count = 6
		return position, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.TaskPositions, map[string]*model.TaskPosition{
		captureID1: {
			CheckPointTs: 3,
			Count:        6,
		},
	})
}

func TestPatchTaskStatus(t *testing.T) {
	state := NewChangefeedReactorState("test1")
	stateTester := NewReactorStateTester(t, state, nil)
	captureID1 := "capture1"
	captureID2 := "capture2"
	state.PatchTaskStatus(captureID1, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		require.Nil(t, status)
		return &model.TaskStatus{
			Tables: map[model.TableID]*model.TableReplicaInfo{45: {StartTs: 1}},
		}, true, nil
	})
	state.PatchTaskStatus(captureID2, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		require.Nil(t, status)
		return &model.TaskStatus{
			Tables: map[model.TableID]*model.TableReplicaInfo{46: {StartTs: 1}},
		}, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.TaskStatuses, map[model.CaptureID]*model.TaskStatus{
		captureID1: {Tables: map[model.TableID]*model.TableReplicaInfo{45: {StartTs: 1}}},
		captureID2: {Tables: map[model.TableID]*model.TableReplicaInfo{46: {StartTs: 1}}},
	})
	state.PatchTaskStatus(captureID1, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[46] = &model.TableReplicaInfo{StartTs: 2}
		return status, true, nil
	})
	state.PatchTaskStatus(captureID2, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[46].StartTs++
		return status, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.TaskStatuses, map[model.CaptureID]*model.TaskStatus{
		captureID1: {Tables: map[model.TableID]*model.TableReplicaInfo{45: {StartTs: 1}, 46: {StartTs: 2}}},
		captureID2: {Tables: map[model.TableID]*model.TableReplicaInfo{46: {StartTs: 2}}},
	})
	state.PatchTaskStatus(captureID2, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.TaskStatuses, map[model.CaptureID]*model.TaskStatus{
		captureID1: {Tables: map[model.TableID]*model.TableReplicaInfo{45: {StartTs: 1}, 46: {StartTs: 2}}},
	})
}

func TestPatchTaskWorkload(t *testing.T) {
	state := NewChangefeedReactorState("test1")
	stateTester := NewReactorStateTester(t, state, nil)
	captureID1 := "capture1"
	captureID2 := "capture2"
	state.PatchTaskWorkload(captureID1, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		require.Nil(t, workload)
		return model.TaskWorkload{45: {Workload: 1}}, true, nil
	})
	state.PatchTaskWorkload(captureID2, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		require.Nil(t, workload)
		return model.TaskWorkload{46: {Workload: 1}}, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.Workloads, map[model.CaptureID]model.TaskWorkload{
		captureID1: {45: {Workload: 1}},
		captureID2: {46: {Workload: 1}},
	})
	state.PatchTaskWorkload(captureID1, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		workload[46] = model.WorkloadInfo{Workload: 2}
		return workload, true, nil
	})
	state.PatchTaskWorkload(captureID2, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		workload[45] = model.WorkloadInfo{Workload: 3}
		return workload, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.Workloads, map[model.CaptureID]model.TaskWorkload{
		captureID1: {45: {Workload: 1}, 46: {Workload: 2}},
		captureID2: {45: {Workload: 3}, 46: {Workload: 1}},
	})
	state.PatchTaskWorkload(captureID2, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.Workloads, map[model.CaptureID]model.TaskWorkload{
		captureID1: {45: {Workload: 1}, 46: {Workload: 2}},
	})
}

func TestGlobalStateUpdate(t *testing.T) {
	testCases := []struct {
		updateKey   []string
		updateValue []string
		expected    GlobalReactorState
	}{
		{ // common case
			updateKey: []string{
				"/tidb/cdc/owner/22317526c4fc9a37",
				"/tidb/cdc/owner/22317526c4fc9a38",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test2",
				"/tidb/cdc/task/workload/55551111/test2",
			},
			updateValue: []string{
				`6bbc01c8-0605-4f86-a0f9-b3119109b225`,
				`55551111`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				`{"46":{"workload":1}}`,
			},
			expected: GlobalReactorState{
				Owner: map[string]struct{}{"22317526c4fc9a37": {}, "22317526c4fc9a38": {}},
				Captures: map[model.CaptureID]*model.CaptureInfo{"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
					ID:            "6bbc01c8-0605-4f86-a0f9-b3119109b225",
					AdvertiseAddr: "127.0.0.1:8300",
				}},
				Changefeeds: map[model.ChangeFeedID]*ChangefeedReactorState{
					"test1": {
						ID:           "test1",
						TaskStatuses: map[string]*model.TaskStatus{},
						TaskPositions: map[model.CaptureID]*model.TaskPosition{
							"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980719742451713, ResolvedTs: 421980720003809281},
						},
						Workloads: map[string]model.TaskWorkload{},
					},
					"test2": {
						ID:            "test2",
						TaskStatuses:  map[string]*model.TaskStatus{},
						TaskPositions: map[model.CaptureID]*model.TaskPosition{},
						Workloads: map[model.CaptureID]model.TaskWorkload{
							"6bbc01c8-0605-4f86-a0f9-b3119109b225": {45: {Workload: 1}},
							"55551111":                             {46: {Workload: 1}},
						},
					},
				},
			},
		},
		{ // testing remove changefeed
			updateKey: []string{
				"/tidb/cdc/owner/22317526c4fc9a37",
				"/tidb/cdc/owner/22317526c4fc9a38",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test2",
				"/tidb/cdc/task/workload/55551111/test2",
				"/tidb/cdc/owner/22317526c4fc9a37",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test2",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
			},
			updateValue: []string{
				`6bbc01c8-0605-4f86-a0f9-b3119109b225`,
				`55551111`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				`{"46":{"workload":1}}`,
				``,
				``,
				``,
				``,
			},
			expected: GlobalReactorState{
				Owner:    map[string]struct{}{"22317526c4fc9a38": {}},
				Captures: map[model.CaptureID]*model.CaptureInfo{},
				Changefeeds: map[model.ChangeFeedID]*ChangefeedReactorState{
					"test2": {
						ID:            "test2",
						TaskStatuses:  map[string]*model.TaskStatus{},
						TaskPositions: map[model.CaptureID]*model.TaskPosition{},
						Workloads: map[model.CaptureID]model.TaskWorkload{
							"55551111": {46: {Workload: 1}},
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		state := NewGlobalState()
		for i, k := range tc.updateKey {
			value := []byte(tc.updateValue[i])
			if len(value) == 0 {
				value = nil
			}
			err := state.Update(util.NewEtcdKey(k), value, false)
			require.Nil(t, err)
		}
		require.True(t, cmp.Equal(state, &tc.expected, cmpopts.IgnoreUnexported(GlobalReactorState{}, ChangefeedReactorState{})),
			cmp.Diff(state, &tc.expected, cmpopts.IgnoreUnexported(GlobalReactorState{}, ChangefeedReactorState{})))
	}
}

func TestCaptureChangeHooks(t *testing.T) {
	state := NewGlobalState()

	var callCount int
	state.onCaptureAdded = func(captureID model.CaptureID, addr string) {
		callCount++
		require.Equal(t, captureID, "capture-1")
		require.Equal(t, addr, "ip-1:8300")
	}
	state.onCaptureRemoved = func(captureID model.CaptureID) {
		callCount++
		require.Equal(t, captureID, "capture-1")
	}

	captureInfo := &model.CaptureInfo{
		ID:            "capture-1",
		AdvertiseAddr: "ip-1:8300",
	}
	captureInfoBytes, err := json.Marshal(captureInfo)
	require.Nil(t, err)

	err = state.Update(util.NewEtcdKey(etcd.CaptureInfoKeyPrefix+"/capture-1"), captureInfoBytes, false)
	require.Nil(t, err)
	require.Equal(t, callCount, 1)

	err = state.Update(util.NewEtcdKey(etcd.CaptureInfoKeyPrefix+"/capture-1"), nil /* delete */, false)
	require.Nil(t, err)
	require.Equal(t, callCount, 2)
}

func TestCheckChangefeedNormal(t *testing.T) {
	state := NewChangefeedReactorState("test1")
	stateTester := NewReactorStateTester(t, state, nil)
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{SinkURI: "123", AdminJobType: model.AdminNone, Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{ResolvedTs: 1, AdminJobType: model.AdminNone}, true, nil
	})
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status.ResolvedTs, uint64(1))

	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		info.AdminJobType = model.AdminStop
		return info, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.ResolvedTs = 2
		return status, true, nil
	})
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status.ResolvedTs, uint64(1))

	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.ResolvedTs = 2
		return status, true, nil
	})
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status.ResolvedTs, uint64(2))
}

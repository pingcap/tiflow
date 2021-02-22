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

package processor

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type stateSuite struct{}

var _ = check.Suite(&stateSuite{})

type mockReactorStatePatcher struct {
	state    orchestrator.ReactorState
	rawState map[util.EtcdKey][]byte
	c        *check.C
}

func newMockReactorStatePatcher(c *check.C, state orchestrator.ReactorState) *mockReactorStatePatcher {
	return &mockReactorStatePatcher{
		state:    state,
		rawState: make(map[util.EtcdKey][]byte),
		c:        c,
	}
}

func (m *mockReactorStatePatcher) applyPatches() {
	patches := m.state.GetPatches()
	m.c.Assert(m.state.GetPatches(), check.HasLen, 0)
	for _, patch := range patches {
		newValue, err := patch.Fun(m.rawState[patch.Key])
		m.c.Assert(err, check.IsNil)
		err = m.state.Update(patch.Key, newValue, false)
		m.c.Assert(err, check.IsNil)
		m.rawState[patch.Key] = newValue
	}
}

func (s *stateSuite) TestChangefeedStateUpdate(c *check.C) {
	defer testleak.AfterTest(c)()
	createTime, err := time.Parse("2006-01-02", "2020-02-02")
	c.Assert(err, check.IsNil)
	testCases := []struct {
		changefeedID string
		captureID    string
		updateKey    []string
		updateValue  []string
		expected     changefeedState
	}{
		{ // common case
			changefeedID: "test1",
			captureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			updateKey: []string{
				"/tidb/cdc/changefeed/info/test1",
				"/tidb/cdc/job/test1",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
			},
			updateValue: []string{
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":".","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"tables":{"45":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
			},
			expected: changefeedState{
				ID:        "test1",
				CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
				Info: &model.ChangeFeedInfo{
					SinkURI:           "blackhole://",
					Opts:              map[string]string{},
					CreateTime:        createTime,
					StartTs:           421980685886554116,
					Engine:            model.SortInMemory,
					SortDir:           ".",
					State:             "normal",
					SyncPointInterval: time.Minute * 10,
					Config: &config.ReplicaConfig{
						CaseSensitive:    true,
						CheckGCSafePoint: true,
						Filter:           &config.FilterConfig{Rules: []string{"*.*"}},
						Mounter:          &config.MounterConfig{WorkerNum: 16},
						Sink:             &config.SinkConfig{Protocol: "default"},
						Cyclic:           &config.CyclicConfig{},
						Scheduler:        &config.SchedulerConfig{Tp: "table-number", PollingTime: -1},
					},
				},
				Status: &model.ChangeFeedStatus{CheckpointTs: 421980719742451713, ResolvedTs: 421980720003809281},
				TaskStatus: &model.TaskStatus{
					Tables: map[int64]*model.TableReplicaInfo{45: {StartTs: 421980685886554116}},
				},
				TaskPosition: &model.TaskPosition{CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
				Workload:     model.TaskWorkload{45: {Workload: 1}},
			},
		},
		{ // testing captureID or changefeedID not match
			changefeedID: "test1",
			captureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
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
				"/tidb/cdc/task/position/fake-capture-id/test1",
				"/tidb/cdc/task/status/fake-capture-id/test1",
				"/tidb/cdc/task/workload/fake-capture-id/test1",
			},
			updateValue: []string{
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":".","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
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
				`fake value`,
				`fake value`,
				`fake value`,
			},
			expected: changefeedState{
				ID:        "test1",
				CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
				Info: &model.ChangeFeedInfo{
					SinkURI:           "blackhole://",
					Opts:              map[string]string{},
					CreateTime:        createTime,
					StartTs:           421980685886554116,
					Engine:            model.SortInMemory,
					SortDir:           ".",
					State:             "normal",
					SyncPointInterval: time.Minute * 10,
					Config: &config.ReplicaConfig{
						CaseSensitive:    true,
						CheckGCSafePoint: true,
						Filter:           &config.FilterConfig{Rules: []string{"*.*"}},
						Mounter:          &config.MounterConfig{WorkerNum: 16},
						Sink:             &config.SinkConfig{Protocol: "default"},
						Cyclic:           &config.CyclicConfig{},
						Scheduler:        &config.SchedulerConfig{Tp: "table-number", PollingTime: -1},
					},
				},
				Status: &model.ChangeFeedStatus{CheckpointTs: 421980719742451713, ResolvedTs: 421980720003809281},
				TaskStatus: &model.TaskStatus{
					Tables: map[int64]*model.TableReplicaInfo{45: {StartTs: 421980685886554116}},
				},
				TaskPosition: &model.TaskPosition{CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
				Workload:     model.TaskWorkload{45: {Workload: 1}},
			},
		},
		{ // testing value is nil
			changefeedID: "test1",
			captureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			updateKey: []string{
				"/tidb/cdc/changefeed/info/test1",
				"/tidb/cdc/job/test1",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				"/tidb/cdc/changefeed/info/test1",
				"/tidb/cdc/job/test1",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
			},
			updateValue: []string{
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":".","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"tables":{"45":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				``,
				``,
				``,
				``,
				``,
				``,
			},
			expected: changefeedState{
				ID:           "test1",
				CaptureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
				Info:         nil,
				Status:       nil,
				TaskStatus:   nil,
				TaskPosition: nil,
				Workload:     nil,
			},
		},
		{ // testing the same key case
			changefeedID: "test1",
			captureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			updateKey: []string{
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
			},
			updateValue: []string{
				`{"tables":{"45":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"tables":{"46":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"tables":{"47":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
			},
			expected: changefeedState{
				ID:        "test1",
				CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
				TaskStatus: &model.TaskStatus{
					Tables: map[int64]*model.TableReplicaInfo{47: {StartTs: 421980685886554116}},
				},
			},
		},
	}
	for _, tc := range testCases {
		state := newChangeFeedState(tc.changefeedID, tc.captureID)
		for i, k := range tc.updateKey {
			value := []byte(tc.updateValue[i])
			if len(value) == 0 {
				value = nil
			}
			err = state.Update(util.NewEtcdKey(k), value, false)
			c.Assert(err, check.IsNil)
		}
		c.Assert(cmp.Equal(state, &tc.expected, cmpopts.IgnoreUnexported(changefeedState{})), check.IsTrue,
			check.Commentf("%s", cmp.Diff(state, &tc.expected, cmpopts.IgnoreUnexported(changefeedState{}))))
	}
}

func (s *stateSuite) TestPatchTaskPosition(c *check.C) {
	defer testleak.AfterTest(c)()
	state := newChangeFeedState("test1", "caputre1")
	patcher := newMockReactorStatePatcher(c, state)
	state.PatchTaskPosition(func(position *model.TaskPosition) (*model.TaskPosition, error) {
		c.Assert(position, check.IsNil)
		return &model.TaskPosition{
			CheckPointTs: 1,
		}, nil
	})
	patcher.applyPatches()
	c.Assert(state.TaskPosition, check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 1,
	})
	state.PatchTaskPosition(func(position *model.TaskPosition) (*model.TaskPosition, error) {
		position.CheckPointTs = 3
		return position, nil
	})
	state.PatchTaskPosition(func(position *model.TaskPosition) (*model.TaskPosition, error) {
		position.ResolvedTs = 2
		return position, nil
	})
	patcher.applyPatches()
	c.Assert(state.TaskPosition, check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 3,
		ResolvedTs:   2,
	})
	state.PatchTaskPosition(func(position *model.TaskPosition) (*model.TaskPosition, error) {
		return nil, nil
	})
	patcher.applyPatches()
	c.Assert(state.TaskPosition, check.IsNil)
}

func (s *stateSuite) TestPatchTaskStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	state := newChangeFeedState("test1", "caputre1")
	patcher := newMockReactorStatePatcher(c, state)
	state.PatchTaskStatus(func(status *model.TaskStatus) (*model.TaskStatus, error) {
		c.Assert(status, check.IsNil)
		return &model.TaskStatus{
			Tables: map[model.TableID]*model.TableReplicaInfo{45: {StartTs: 1}},
		}, nil
	})
	patcher.applyPatches()
	c.Assert(state.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{45: {StartTs: 1}},
	})
	state.PatchTaskStatus(func(status *model.TaskStatus) (*model.TaskStatus, error) {
		status.Tables[46] = &model.TableReplicaInfo{StartTs: 2}
		return status, nil
	})
	state.PatchTaskStatus(func(status *model.TaskStatus) (*model.TaskStatus, error) {
		status.Tables[45].StartTs++
		return status, nil
	})
	patcher.applyPatches()
	c.Assert(state.TaskStatus, check.DeepEquals, &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{45: {StartTs: 2}, 46: {StartTs: 2}},
	})
	state.PatchTaskStatus(func(status *model.TaskStatus) (*model.TaskStatus, error) {
		return nil, nil
	})
	patcher.applyPatches()
	c.Assert(state.TaskStatus, check.IsNil)
}

func (s *stateSuite) TestPatchTaskWorkload(c *check.C) {
	defer testleak.AfterTest(c)()
	state := newChangeFeedState("test1", "caputre1")
	patcher := newMockReactorStatePatcher(c, state)
	state.PatchTaskWorkload(func(workload model.TaskWorkload) (model.TaskWorkload, error) {
		c.Assert(workload, check.IsNil)
		return model.TaskWorkload{45: {Workload: 1}}, nil
	})
	patcher.applyPatches()
	c.Assert(state.Workload, check.DeepEquals, model.TaskWorkload{45: {Workload: 1}})
	state.PatchTaskWorkload(func(workload model.TaskWorkload) (model.TaskWorkload, error) {
		workload[46] = model.WorkloadInfo{Workload: 2}
		return workload, nil
	})
	state.PatchTaskWorkload(func(workload model.TaskWorkload) (model.TaskWorkload, error) {
		workload[45] = model.WorkloadInfo{Workload: 3}
		return workload, nil
	})
	patcher.applyPatches()
	c.Assert(state.Workload, check.DeepEquals, model.TaskWorkload{45: {Workload: 3}, 46: {Workload: 2}})
	state.PatchTaskWorkload(func(workload model.TaskWorkload) (model.TaskWorkload, error) {
		return nil, nil
	})
	patcher.applyPatches()
	c.Assert(state.Workload, check.IsNil)
}

func (s *stateSuite) TestGlobalStateUpdate(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		captureID   string
		updateKey   []string
		updateValue []string
		expected    globalState
	}{
		{ // common case
			captureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			updateKey: []string{
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test2",
			},
			updateValue: []string{
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
			},
			expected: globalState{
				CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
				Changefeeds: map[model.ChangeFeedID]*changefeedState{
					"test1": {
						ID:           "test1",
						CaptureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
						TaskPosition: &model.TaskPosition{CheckPointTs: 421980719742451713, ResolvedTs: 421980720003809281},
					},
					"test2": {
						ID:        "test2",
						CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
						Workload:  model.TaskWorkload{45: {Workload: 1}},
					},
				},
			},
		}, { // testing captureID not match
			captureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			updateKey: []string{
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b226/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b226/test2",
			},
			updateValue: []string{
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
			},
			expected: globalState{
				CaptureID:   "6bbc01c8-0605-4f86-a0f9-b3119109b225",
				Changefeeds: map[model.ChangeFeedID]*changefeedState{},
			},
		}, { // testing remove changefeed
			captureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			updateKey: []string{
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				"/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test2",
				"/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
			},
			updateValue: []string{
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				"",
			},
			expected: globalState{
				CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
				Changefeeds: map[model.ChangeFeedID]*changefeedState{
					"test2": {
						ID:        "test2",
						CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
						Workload:  model.TaskWorkload{45: {Workload: 1}},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		state := NewGlobalState(tc.captureID)
		for i, k := range tc.updateKey {
			value := []byte(tc.updateValue[i])
			if len(value) == 0 {
				value = nil
			}
			err := state.Update(util.NewEtcdKey(k), value, false)
			c.Assert(err, check.IsNil)
		}
		c.Assert(cmp.Equal(state, &tc.expected, cmp.AllowUnexported(globalState{}), cmpopts.IgnoreUnexported(changefeedState{})), check.IsTrue,
			check.Commentf("%s", cmp.Diff(state, &tc.expected, cmp.AllowUnexported(globalState{}), cmpopts.IgnoreUnexported(changefeedState{}))))
	}
}

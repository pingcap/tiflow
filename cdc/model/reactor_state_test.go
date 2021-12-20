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

package model

import (
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/orchestrator/util"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type stateSuite struct{}

var _ = check.Suite(&stateSuite{})

func (s *stateSuite) TestCheckCaptureAlive(c *check.C) {
	defer testleak.AfterTest(c)()
	state := NewChangefeedReactorState("test")
	stateTester := orchestrator.NewReactorStateTester(c, state, nil)
	state.CheckCaptureAlive("6bbc01c8-0605-4f86-a0f9-b3119109b225")
	c.Assert(stateTester.ApplyPatches(), check.ErrorMatches, ".*[CDC:ErrLeaseExpired].*")
	err := stateTester.Update("/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225", []byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`))
	c.Assert(err, check.IsNil)
	state.CheckCaptureAlive("6bbc01c8-0605-4f86-a0f9-b3119109b225")
	stateTester.MustApplyPatches()
}

func (s *stateSuite) TestChangefeedStateUpdate(c *check.C) {
	defer testleak.AfterTest(c)()
	createTime, err := time.Parse("2006-01-02", "2020-02-02")
	c.Assert(err, check.IsNil)
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
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":"","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"tables":{"45":{"start-ts":421980685886554116,"mark-table-id":0}},"operation":null,"admin-job-type":0}`,
				`{"45":{"workload":1}}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
			},
			expected: ChangefeedReactorState{
				ID: "test1",
				Info: &ChangeFeedInfo{
					SinkURI:           "blackhole://",
					Opts:              map[string]string{},
					CreateTime:        createTime,
					StartTs:           421980685886554116,
					Engine:            SortInMemory,
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
				Status: &ChangeFeedStatus{CheckpointTs: 421980719742451713, ResolvedTs: 421980720003809281},
				TaskStatuses: map[CaptureID]*TaskStatus{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
						Tables: map[int64]*TableReplicaInfo{45: {StartTs: 421980685886554116}},
					},
				},
				TaskPositions: map[CaptureID]*TaskPosition{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
				},
				Workloads: map[CaptureID]TaskWorkload{
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
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":"","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
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
				Info: &ChangeFeedInfo{
					SinkURI:           "blackhole://",
					Opts:              map[string]string{},
					CreateTime:        createTime,
					StartTs:           421980685886554116,
					Engine:            SortInMemory,
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
				Status: &ChangeFeedStatus{CheckpointTs: 421980719742451713, ResolvedTs: 421980720003809281},
				TaskStatuses: map[CaptureID]*TaskStatus{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
						Tables: map[int64]*TableReplicaInfo{45: {StartTs: 421980685886554116}},
					},
					"666777888": {
						Tables: map[int64]*TableReplicaInfo{46: {StartTs: 412341234}},
					},
				},
				TaskPositions: map[CaptureID]*TaskPosition{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
					"666777888":                            {CheckPointTs: 11332244, ResolvedTs: 312321, Count: 8},
				},
				Workloads: map[CaptureID]TaskWorkload{
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
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":"","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
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
				Info: &ChangeFeedInfo{
					SinkURI:           "blackhole://",
					Opts:              map[string]string{},
					CreateTime:        createTime,
					StartTs:           421980685886554116,
					Engine:            SortInMemory,
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
				Status: &ChangeFeedStatus{CheckpointTs: 421980719742451713, ResolvedTs: 421980720003809281},
				TaskStatuses: map[CaptureID]*TaskStatus{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
						Tables: map[int64]*TableReplicaInfo{45: {StartTs: 421980685886554116}},
					},
				},
				TaskPositions: map[CaptureID]*TaskPosition{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
				},
				Workloads: map[CaptureID]TaskWorkload{
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
				`{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":421980685886554116,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":"","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
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
				TaskStatuses: map[CaptureID]*TaskStatus{},
				TaskPositions: map[CaptureID]*TaskPosition{
					"666777888": {CheckPointTs: 11332244, ResolvedTs: 312321, Count: 8},
				},
				Workloads: map[CaptureID]TaskWorkload{},
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
				TaskStatuses: map[CaptureID]*TaskStatus{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
						Tables: map[int64]*TableReplicaInfo{47: {StartTs: 421980685886554116}},
					},
				},
				TaskPositions: map[CaptureID]*TaskPosition{},
				Workloads:     map[CaptureID]TaskWorkload{},
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
			c.Assert(err, check.IsNil)
		}
		c.Assert(cmp.Equal(state, &tc.expected, cmpopts.IgnoreUnexported(ChangefeedReactorState{})), check.IsTrue,
			check.Commentf("%d,%s", i, cmp.Diff(state, &tc.expected, cmpopts.IgnoreUnexported(ChangefeedReactorState{}))))
	}
}

func (s *stateSuite) TestPatchInfo(c *check.C) {
	defer testleak.AfterTest(c)()
	state := NewChangefeedReactorState("test1")
	stateTester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *ChangeFeedInfo) (*ChangeFeedInfo, bool, error) {
		c.Assert(info, check.IsNil)
		return &ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	stateTester.MustApplyPatches()
	defaultConfig := config.GetDefaultReplicaConfig()
	c.Assert(state.Info, check.DeepEquals, &ChangeFeedInfo{
		SinkURI: "123",
		Engine:  SortUnified,
		Config: &config.ReplicaConfig{
			Filter:    defaultConfig.Filter,
			Mounter:   defaultConfig.Mounter,
			Sink:      defaultConfig.Sink,
			Cyclic:    defaultConfig.Cyclic,
			Scheduler: defaultConfig.Scheduler,
		},
	})
	state.PatchInfo(func(info *ChangeFeedInfo) (*ChangeFeedInfo, bool, error) {
		info.StartTs = 6
		return info, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.Info, check.DeepEquals, &ChangeFeedInfo{
		SinkURI: "123",
		StartTs: 6,
		Engine:  SortUnified,
		Config: &config.ReplicaConfig{
			Filter:    defaultConfig.Filter,
			Mounter:   defaultConfig.Mounter,
			Sink:      defaultConfig.Sink,
			Cyclic:    defaultConfig.Cyclic,
			Scheduler: defaultConfig.Scheduler,
		},
	})
	state.PatchInfo(func(info *ChangeFeedInfo) (*ChangeFeedInfo, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.Info, check.IsNil)
}

func (s *stateSuite) TestPatchStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	state := NewChangefeedReactorState("test1")
	stateTester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchStatus(func(status *ChangeFeedStatus) (*ChangeFeedStatus, bool, error) {
		c.Assert(status, check.IsNil)
		return &ChangeFeedStatus{CheckpointTs: 5}, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.Status, check.DeepEquals, &ChangeFeedStatus{CheckpointTs: 5})
	state.PatchStatus(func(status *ChangeFeedStatus) (*ChangeFeedStatus, bool, error) {
		status.ResolvedTs = 6
		return status, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.Status, check.DeepEquals, &ChangeFeedStatus{CheckpointTs: 5, ResolvedTs: 6})
	state.PatchStatus(func(status *ChangeFeedStatus) (*ChangeFeedStatus, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.Status, check.IsNil)
}

func (s *stateSuite) TestPatchTaskPosition(c *check.C) {
	defer testleak.AfterTest(c)()
	state := NewChangefeedReactorState("test1")
	stateTester := orchestrator.NewReactorStateTester(c, state, nil)
	captureID1 := "capture1"
	captureID2 := "capture2"
	state.PatchTaskPosition(captureID1, func(position *TaskPosition) (*TaskPosition, bool, error) {
		c.Assert(position, check.IsNil)
		return &TaskPosition{
			CheckPointTs: 1,
		}, true, nil
	})
	state.PatchTaskPosition(captureID2, func(position *TaskPosition) (*TaskPosition, bool, error) {
		c.Assert(position, check.IsNil)
		return &TaskPosition{
			CheckPointTs: 2,
		}, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.TaskPositions, check.DeepEquals, map[string]*TaskPosition{
		captureID1: {
			CheckPointTs: 1,
		},
		captureID2: {
			CheckPointTs: 2,
		},
	})
	state.PatchTaskPosition(captureID1, func(position *TaskPosition) (*TaskPosition, bool, error) {
		position.CheckPointTs = 3
		return position, true, nil
	})
	state.PatchTaskPosition(captureID2, func(position *TaskPosition) (*TaskPosition, bool, error) {
		position.ResolvedTs = 2
		return position, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.TaskPositions, check.DeepEquals, map[string]*TaskPosition{
		captureID1: {
			CheckPointTs: 3,
		},
		captureID2: {
			CheckPointTs: 2,
			ResolvedTs:   2,
		},
	})
	state.PatchTaskPosition(captureID1, func(position *TaskPosition) (*TaskPosition, bool, error) {
		return nil, false, nil
	})
	state.PatchTaskPosition(captureID2, func(position *TaskPosition) (*TaskPosition, bool, error) {
		return nil, true, nil
	})
	state.PatchTaskPosition(captureID1, func(position *TaskPosition) (*TaskPosition, bool, error) {
		position.Count = 6
		return position, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.TaskPositions, check.DeepEquals, map[string]*TaskPosition{
		captureID1: {
			CheckPointTs: 3,
			Count:        6,
		},
	})
}

func (s *stateSuite) TestPatchTaskStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	state := NewChangefeedReactorState("test1")
	stateTester := orchestrator.NewReactorStateTester(c, state, nil)
	captureID1 := "capture1"
	captureID2 := "capture2"
	state.PatchTaskStatus(captureID1, func(status *TaskStatus) (*TaskStatus, bool, error) {
		c.Assert(status, check.IsNil)
		return &TaskStatus{
			Tables: map[TableID]*TableReplicaInfo{45: {StartTs: 1}},
		}, true, nil
	})
	state.PatchTaskStatus(captureID2, func(status *TaskStatus) (*TaskStatus, bool, error) {
		c.Assert(status, check.IsNil)
		return &TaskStatus{
			Tables: map[TableID]*TableReplicaInfo{46: {StartTs: 1}},
		}, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.TaskStatuses, check.DeepEquals, map[CaptureID]*TaskStatus{
		captureID1: {Tables: map[TableID]*TableReplicaInfo{45: {StartTs: 1}}},
		captureID2: {Tables: map[TableID]*TableReplicaInfo{46: {StartTs: 1}}},
	})
	state.PatchTaskStatus(captureID1, func(status *TaskStatus) (*TaskStatus, bool, error) {
		status.Tables[46] = &TableReplicaInfo{StartTs: 2}
		return status, true, nil
	})
	state.PatchTaskStatus(captureID2, func(status *TaskStatus) (*TaskStatus, bool, error) {
		status.Tables[46].StartTs++
		return status, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.TaskStatuses, check.DeepEquals, map[CaptureID]*TaskStatus{
		captureID1: {Tables: map[TableID]*TableReplicaInfo{45: {StartTs: 1}, 46: {StartTs: 2}}},
		captureID2: {Tables: map[TableID]*TableReplicaInfo{46: {StartTs: 2}}},
	})
	state.PatchTaskStatus(captureID2, func(status *TaskStatus) (*TaskStatus, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.TaskStatuses, check.DeepEquals, map[CaptureID]*TaskStatus{
		captureID1: {Tables: map[TableID]*TableReplicaInfo{45: {StartTs: 1}, 46: {StartTs: 2}}},
	})
}

func (s *stateSuite) TestPatchTaskWorkload(c *check.C) {
	defer testleak.AfterTest(c)()
	state := NewChangefeedReactorState("test1")
	stateTester := orchestrator.NewReactorStateTester(c, state, nil)
	captureID1 := "capture1"
	captureID2 := "capture2"
	state.PatchTaskWorkload(captureID1, func(workload TaskWorkload) (TaskWorkload, bool, error) {
		c.Assert(workload, check.IsNil)
		return TaskWorkload{45: {Workload: 1}}, true, nil
	})
	state.PatchTaskWorkload(captureID2, func(workload TaskWorkload) (TaskWorkload, bool, error) {
		c.Assert(workload, check.IsNil)
		return TaskWorkload{46: {Workload: 1}}, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.Workloads, check.DeepEquals, map[CaptureID]TaskWorkload{
		captureID1: {45: {Workload: 1}},
		captureID2: {46: {Workload: 1}},
	})
	state.PatchTaskWorkload(captureID1, func(workload TaskWorkload) (TaskWorkload, bool, error) {
		workload[46] = WorkloadInfo{Workload: 2}
		return workload, true, nil
	})
	state.PatchTaskWorkload(captureID2, func(workload TaskWorkload) (TaskWorkload, bool, error) {
		workload[45] = WorkloadInfo{Workload: 3}
		return workload, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.Workloads, check.DeepEquals, map[CaptureID]TaskWorkload{
		captureID1: {45: {Workload: 1}, 46: {Workload: 2}},
		captureID2: {45: {Workload: 3}, 46: {Workload: 1}},
	})
	state.PatchTaskWorkload(captureID2, func(workload TaskWorkload) (TaskWorkload, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	c.Assert(state.Workloads, check.DeepEquals, map[CaptureID]TaskWorkload{
		captureID1: {45: {Workload: 1}, 46: {Workload: 2}},
	})
}

func (s *stateSuite) TestGlobalStateUpdate(c *check.C) {
	defer testleak.AfterTest(c)()
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
				Captures: map[CaptureID]*CaptureInfo{"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
					ID:            "6bbc01c8-0605-4f86-a0f9-b3119109b225",
					AdvertiseAddr: "127.0.0.1:8300",
				}},
				Changefeeds: map[ChangeFeedID]*ChangefeedReactorState{
					"test1": {
						ID:           "test1",
						TaskStatuses: map[string]*TaskStatus{},
						TaskPositions: map[CaptureID]*TaskPosition{
							"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980719742451713, ResolvedTs: 421980720003809281},
						},
						Workloads: map[string]TaskWorkload{},
					},
					"test2": {
						ID:            "test2",
						TaskStatuses:  map[string]*TaskStatus{},
						TaskPositions: map[CaptureID]*TaskPosition{},
						Workloads: map[CaptureID]TaskWorkload{
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
				Captures: map[CaptureID]*CaptureInfo{},
				Changefeeds: map[ChangeFeedID]*ChangefeedReactorState{
					"test2": {
						ID:            "test2",
						TaskStatuses:  map[string]*TaskStatus{},
						TaskPositions: map[CaptureID]*TaskPosition{},
						Workloads: map[CaptureID]TaskWorkload{
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
			c.Assert(err, check.IsNil)
		}
		c.Assert(cmp.Equal(state, &tc.expected, cmpopts.IgnoreUnexported(GlobalReactorState{}, ChangefeedReactorState{})), check.IsTrue,
			check.Commentf("%s", cmp.Diff(state, &tc.expected, cmpopts.IgnoreUnexported(GlobalReactorState{}, ChangefeedReactorState{}))))
	}
}

func (s *stateSuite) TestCheckChangefeedNormal(c *check.C) {
	defer testleak.AfterTest(c)()
	state := NewChangefeedReactorState("test1")
	stateTester := orchestrator.NewReactorStateTester(c, state, nil)
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	state.PatchInfo(func(info *ChangeFeedInfo) (*ChangeFeedInfo, bool, error) {
		return &ChangeFeedInfo{SinkURI: "123", AdminJobType: AdminNone, Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *ChangeFeedStatus) (*ChangeFeedStatus, bool, error) {
		return &ChangeFeedStatus{ResolvedTs: 1, AdminJobType: AdminNone}, true, nil
	})
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	c.Assert(state.Status.ResolvedTs, check.Equals, uint64(1))

	state.PatchInfo(func(info *ChangeFeedInfo) (*ChangeFeedInfo, bool, error) {
		info.AdminJobType = AdminStop
		return info, true, nil
	})
	state.PatchStatus(func(status *ChangeFeedStatus) (*ChangeFeedStatus, bool, error) {
		status.ResolvedTs = 2
		return status, true, nil
	})
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	c.Assert(state.Status.ResolvedTs, check.Equals, uint64(1))

	state.PatchStatus(func(status *ChangeFeedStatus) (*ChangeFeedStatus, bool, error) {
		status.ResolvedTs = 2
		return status, true, nil
	})
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	c.Assert(state.Status.ResolvedTs, check.Equals, uint64(2))
}

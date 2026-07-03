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
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestCheckCaptureAlive(t *testing.T) {
	state := NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID("test"))
	stateTester := NewReactorStateTester(t, state, nil)
	state.CheckCaptureAlive("6bbc01c8-0605-4f86-a0f9-b3119109b225")
	require.Contains(t, stateTester.ApplyPatches().Error(), "[CDC:ErrLeaseExpired]")
	err := stateTester.Update(etcd.DefaultClusterAndMetaPrefix+
		"/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		[]byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`))
	require.Nil(t, err)
	state.CheckCaptureAlive("6bbc01c8-0605-4f86-a0f9-b3119109b225")
	stateTester.MustApplyPatches()
}

func TestChangefeedStateUpdate(t *testing.T) {
	changefeedInfo := `
{
    "sink-uri": "blackhole://",
    "opts": {},
    "create-time": "2020-02-02T00:00:00.000000+00:00",
    "start-ts": 421980685886554116,
    "target-ts": 0,
    "admin-job-type": 0,
    "sort-engine": "memory",
    "sort-dir": "",
    "config": {
        "case-sensitive": true,
        "force-replicate": false,
        "check-gc-safe-point": true,
        "filter": {
            "rules": [
                "*.*"
            ],
            "ignore-txn-start-ts": null
        },
        "mounter": {
            "worker-num": 16
        }
    },
    "state": "normal",
    "history": null,
    "error": null,
    "sync-point-enabled": false,
    "sync-point-interval": 600000000000
}
`
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
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/info/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/status/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				etcd.DefaultClusterAndMetaPrefix +
					"/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
			},
			updateValue: []string{
				changefeedInfo,
				`{"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
			},
			expected: ChangefeedReactorState{
				ClusterID: etcd.DefaultCDCClusterID,
				ID:        model.DefaultChangeFeedID("test1"),
				Info: &model.ChangeFeedInfo{
					SinkURI:    "blackhole://",
					CreateTime: createTime,
					StartTs:    421980685886554116,
					Engine:     model.SortInMemory,
					State:      "normal",
					Config: &config.ReplicaConfig{
						CaseSensitive:    true,
						CheckGCSafePoint: true,
						Filter:           &config.FilterConfig{Rules: []string{"*.*"}},
						Mounter:          &config.MounterConfig{WorkerNum: 16},
						Scheduler:        config.GetDefaultReplicaConfig().Scheduler,
						Sink: &config.SinkConfig{
							Terminator:                       putil.AddressOf(config.CRLF),
							AdvanceTimeoutInSec:              putil.AddressOf(uint(150)),
							CSVConfig:                        config.GetDefaultReplicaConfig().Sink.CSVConfig,
							EncoderConcurrency:               config.GetDefaultReplicaConfig().Sink.EncoderConcurrency,
							DateSeparator:                    config.GetDefaultReplicaConfig().Sink.DateSeparator,
							EnablePartitionSeparator:         config.GetDefaultReplicaConfig().Sink.EnablePartitionSeparator,
							EnableKafkaSinkV2:                config.GetDefaultReplicaConfig().Sink.EnableKafkaSinkV2,
							OnlyOutputUpdatedColumns:         config.GetDefaultReplicaConfig().Sink.OnlyOutputUpdatedColumns,
							ContentCompatible:                config.GetDefaultReplicaConfig().Sink.ContentCompatible,
							DeleteOnlyOutputHandleKeyColumns: config.GetDefaultReplicaConfig().Sink.DeleteOnlyOutputHandleKeyColumns,
							SendBootstrapIntervalInSec:       config.GetDefaultReplicaConfig().Sink.SendBootstrapIntervalInSec,
							SendBootstrapInMsgCount:          config.GetDefaultReplicaConfig().Sink.SendBootstrapInMsgCount,
							SendBootstrapToAllPartition:      config.GetDefaultReplicaConfig().Sink.SendBootstrapToAllPartition,
							SendAllBootstrapAtStart:          config.GetDefaultReplicaConfig().Sink.SendAllBootstrapAtStart,
							OpenProtocol:                     config.GetDefaultReplicaConfig().Sink.OpenProtocol,
						},
						Consistent: config.GetDefaultReplicaConfig().Consistent,
						Integrity:  config.GetDefaultReplicaConfig().Integrity,
						ChangefeedErrorStuckDuration: config.
							GetDefaultReplicaConfig().ChangefeedErrorStuckDuration,
						SQLMode:      config.GetDefaultReplicaConfig().SQLMode,
						SyncedStatus: config.GetDefaultReplicaConfig().SyncedStatus,
					},
				},
				Status: &model.ChangeFeedStatus{CheckpointTs: 421980719742451713},
				TaskPositions: map[model.CaptureID]*model.TaskPosition{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
				},
			},
		},
		{ // test multiple capture
			changefeedID: "test1",
			updateKey: []string{
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/info/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/status/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				etcd.DefaultClusterAndMetaPrefix +
					"/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/666777888/test1",
				etcd.DefaultClusterAndMetaPrefix +
					"/capture/666777888",
			},
			updateValue: []string{
				changefeedInfo,
				`{"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`{"checkpoint-ts":11332244,"resolved-ts":312321,"count":8,"error":null}`,
				`{"id":"666777888","address":"127.0.0.1:8300"}`,
			},
			expected: ChangefeedReactorState{
				ClusterID: etcd.DefaultCDCClusterID,
				ID:        model.DefaultChangeFeedID("test1"),
				Info: &model.ChangeFeedInfo{
					SinkURI:    "blackhole://",
					CreateTime: createTime,
					StartTs:    421980685886554116,
					Engine:     model.SortInMemory,
					State:      "normal",
					Config: &config.ReplicaConfig{
						CaseSensitive:    true,
						CheckGCSafePoint: true,
						Filter:           &config.FilterConfig{Rules: []string{"*.*"}},
						Mounter:          &config.MounterConfig{WorkerNum: 16},
						Sink: &config.SinkConfig{
							Terminator:                       putil.AddressOf(config.CRLF),
							AdvanceTimeoutInSec:              putil.AddressOf(uint(150)),
							CSVConfig:                        config.GetDefaultReplicaConfig().Sink.CSVConfig,
							EncoderConcurrency:               config.GetDefaultReplicaConfig().Sink.EncoderConcurrency,
							DateSeparator:                    config.GetDefaultReplicaConfig().Sink.DateSeparator,
							EnablePartitionSeparator:         config.GetDefaultReplicaConfig().Sink.EnablePartitionSeparator,
							EnableKafkaSinkV2:                config.GetDefaultReplicaConfig().Sink.EnableKafkaSinkV2,
							OnlyOutputUpdatedColumns:         config.GetDefaultReplicaConfig().Sink.OnlyOutputUpdatedColumns,
							ContentCompatible:                config.GetDefaultReplicaConfig().Sink.ContentCompatible,
							DeleteOnlyOutputHandleKeyColumns: config.GetDefaultReplicaConfig().Sink.DeleteOnlyOutputHandleKeyColumns,
							SendBootstrapIntervalInSec:       config.GetDefaultReplicaConfig().Sink.SendBootstrapIntervalInSec,
							SendBootstrapInMsgCount:          config.GetDefaultReplicaConfig().Sink.SendBootstrapInMsgCount,
							SendBootstrapToAllPartition:      config.GetDefaultReplicaConfig().Sink.SendBootstrapToAllPartition,
							SendAllBootstrapAtStart:          config.GetDefaultReplicaConfig().Sink.SendAllBootstrapAtStart,
							OpenProtocol:                     config.GetDefaultReplicaConfig().Sink.OpenProtocol,
						},
						Scheduler:  config.GetDefaultReplicaConfig().Scheduler,
						Integrity:  config.GetDefaultReplicaConfig().Integrity,
						Consistent: config.GetDefaultReplicaConfig().Consistent,
						ChangefeedErrorStuckDuration: config.
							GetDefaultReplicaConfig().ChangefeedErrorStuckDuration,
						SQLMode:      config.GetDefaultReplicaConfig().SQLMode,
						SyncedStatus: config.GetDefaultReplicaConfig().SyncedStatus,
					},
				},
				Status: &model.ChangeFeedStatus{CheckpointTs: 421980719742451713},
				TaskPositions: map[model.CaptureID]*model.TaskPosition{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
					"666777888":                            {CheckPointTs: 11332244, ResolvedTs: 312321, Count: 8},
				},
			},
		},
		{ // testing changefeedID not match
			changefeedID: "test1",
			updateKey: []string{
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/info/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/status/test1",

				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				etcd.DefaultClusterAndMetaPrefix +
					"/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/info/test-fake",
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/status/test-fake",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test-fake",
			},
			updateValue: []string{
				changefeedInfo,
				`{"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`fake value`,
				`fake value`,
				`fake value`,
			},
			expected: ChangefeedReactorState{
				ClusterID: etcd.DefaultCDCClusterID,
				ID:        model.DefaultChangeFeedID("test1"),
				Info: &model.ChangeFeedInfo{
					SinkURI:    "blackhole://",
					CreateTime: createTime,
					StartTs:    421980685886554116,
					Engine:     model.SortInMemory,
					State:      "normal",
					Config: &config.ReplicaConfig{
						CaseSensitive:    true,
						CheckGCSafePoint: true,
						Filter:           &config.FilterConfig{Rules: []string{"*.*"}},
						Mounter:          &config.MounterConfig{WorkerNum: 16},
						Sink: &config.SinkConfig{
							Terminator:                       putil.AddressOf(config.CRLF),
							AdvanceTimeoutInSec:              putil.AddressOf(uint(150)),
							EncoderConcurrency:               config.GetDefaultReplicaConfig().Sink.EncoderConcurrency,
							CSVConfig:                        config.GetDefaultReplicaConfig().Sink.CSVConfig,
							DateSeparator:                    config.GetDefaultReplicaConfig().Sink.DateSeparator,
							EnablePartitionSeparator:         config.GetDefaultReplicaConfig().Sink.EnablePartitionSeparator,
							EnableKafkaSinkV2:                config.GetDefaultReplicaConfig().Sink.EnableKafkaSinkV2,
							OnlyOutputUpdatedColumns:         config.GetDefaultReplicaConfig().Sink.OnlyOutputUpdatedColumns,
							ContentCompatible:                config.GetDefaultReplicaConfig().Sink.ContentCompatible,
							DeleteOnlyOutputHandleKeyColumns: config.GetDefaultReplicaConfig().Sink.DeleteOnlyOutputHandleKeyColumns,
							SendBootstrapIntervalInSec:       config.GetDefaultReplicaConfig().Sink.SendBootstrapIntervalInSec,
							SendBootstrapInMsgCount:          config.GetDefaultReplicaConfig().Sink.SendBootstrapInMsgCount,
							SendBootstrapToAllPartition:      config.GetDefaultReplicaConfig().Sink.SendBootstrapToAllPartition,
							SendAllBootstrapAtStart:          config.GetDefaultReplicaConfig().Sink.SendAllBootstrapAtStart,
							OpenProtocol:                     config.GetDefaultReplicaConfig().Sink.OpenProtocol,
						},
						Consistent: config.GetDefaultReplicaConfig().Consistent,
						Scheduler:  config.GetDefaultReplicaConfig().Scheduler,
						Integrity:  config.GetDefaultReplicaConfig().Integrity,
						ChangefeedErrorStuckDuration: config.
							GetDefaultReplicaConfig().ChangefeedErrorStuckDuration,
						SQLMode:      config.GetDefaultReplicaConfig().SQLMode,
						SyncedStatus: config.GetDefaultReplicaConfig().SyncedStatus,
					},
				},
				Status: &model.ChangeFeedStatus{CheckpointTs: 421980719742451713},
				TaskPositions: map[model.CaptureID]*model.TaskPosition{
					"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980720003809281, ResolvedTs: 421980720003809281},
				},
			},
		},
		{ // testing value is nil
			changefeedID: "test1",
			updateKey: []string{
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/info/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/status/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				etcd.DefaultClusterAndMetaPrefix +
					"/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/666777888/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/info/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/changefeed/status/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				etcd.DefaultClusterAndMetaPrefix +
					"/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
			},
			updateValue: []string{
				changefeedInfo,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,"admin-job-type":0}`,
				`{"checkpoint-ts":421980720003809281,"resolved-ts":421980720003809281,"count":0,"error":null}`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`{"checkpoint-ts":11332244,"resolved-ts":312321,"count":8,"error":null}`,
				``,
				``,
				``,
				``,
				``,
				``,
			},
			expected: ChangefeedReactorState{
				ClusterID: etcd.DefaultCDCClusterID,
				ID:        model.DefaultChangeFeedID("test1"),
				Info:      nil,
				Status:    nil,
				TaskPositions: map[model.CaptureID]*model.TaskPosition{
					"666777888": {CheckPointTs: 11332244, ResolvedTs: 312321, Count: 8},
				},
			},
		},
	}
	for i, tc := range testCases {
		state := NewChangefeedReactorState(etcd.DefaultCDCClusterID,
			model.DefaultChangeFeedID(tc.changefeedID))
		for i, k := range tc.updateKey {
			value := []byte(tc.updateValue[i])
			if len(value) == 0 {
				value = nil
			}
			err = state.Update(util.NewEtcdKey(k), value, false)
			require.Nil(t, err)
		}
		require.True(t, cmp.Equal(
			state, &tc.expected,
			cmpopts.IgnoreUnexported(ChangefeedReactorState{}),
		),
			fmt.Sprintf("%d,%s", i, cmp.Diff(state, &tc.expected, cmpopts.IgnoreUnexported(ChangefeedReactorState{}))))
	}
}

func TestPatchInfo(t *testing.T) {
	state := NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID("test1"))
	stateTester := NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	stateTester.MustApplyPatches()
	defaultConfig := config.GetDefaultReplicaConfig()
	cfInfo := &model.ChangeFeedInfo{
		SinkURI: "123",
		Engine:  model.SortUnified,
		Config: &config.ReplicaConfig{
			Filter:                       defaultConfig.Filter,
			Mounter:                      defaultConfig.Mounter,
			Sink:                         defaultConfig.Sink,
			Consistent:                   defaultConfig.Consistent,
			Scheduler:                    defaultConfig.Scheduler,
			Integrity:                    defaultConfig.Integrity,
			ChangefeedErrorStuckDuration: defaultConfig.ChangefeedErrorStuckDuration,
			SQLMode:                      defaultConfig.SQLMode,
			SyncedStatus:                 defaultConfig.SyncedStatus,
		},
	}
	cfInfo.RmUnusedFields()
	require.Equal(t, state.Info, cfInfo)

	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		info.StartTs = 6
		return info, true, nil
	})
	stateTester.MustApplyPatches()
	cfInfo = &model.ChangeFeedInfo{
		SinkURI: "123",
		StartTs: 6,
		Engine:  model.SortUnified,
		Config: &config.ReplicaConfig{
			Filter:                       defaultConfig.Filter,
			Mounter:                      defaultConfig.Mounter,
			Sink:                         defaultConfig.Sink,
			Consistent:                   defaultConfig.Consistent,
			Scheduler:                    defaultConfig.Scheduler,
			Integrity:                    defaultConfig.Integrity,
			ChangefeedErrorStuckDuration: defaultConfig.ChangefeedErrorStuckDuration,
			SQLMode:                      defaultConfig.SQLMode,
			SyncedStatus:                 defaultConfig.SyncedStatus,
		},
	}
	cfInfo.RmUnusedFields()
	require.Equal(t, state.Info, cfInfo)

	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	require.Nil(t, state.Info)
}

func TestPatchStatus(t *testing.T) {
	state := NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID("test1"))
	stateTester := NewReactorStateTester(t, state, nil)
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{CheckpointTs: 5}, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status, &model.ChangeFeedStatus{CheckpointTs: 5})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 6
		return status, true, nil
	})
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status, &model.ChangeFeedStatus{CheckpointTs: 6})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return nil, true, nil
	})
	stateTester.MustApplyPatches()
	require.Nil(t, state.Status)
}

func TestPatchTaskPosition(t *testing.T) {
	state := NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID("test1"))
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

func TestGlobalStateUpdate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		updateKey   []string
		updateValue []string
		expected    GlobalReactorState
		timeout     int
	}{
		{ // common case
			updateKey: []string{
				etcd.DefaultClusterAndMetaPrefix +
					"/owner/22317526c4fc9a37",
				etcd.DefaultClusterAndMetaPrefix +
					"/owner/22317526c4fc9a38",
				etcd.DefaultClusterAndMetaPrefix +
					"/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test2",
				etcd.DefaultClusterAndNamespacePrefix +
					"/upstream/12345",
			},
			updateValue: []string{
				`6bbc01c8-0605-4f86-a0f9-b3119109b225`,
				`55551111`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,
"admin-job-type":0}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,
"admin-job-type":0}`,
				`{}`,
			},
			expected: GlobalReactorState{
				ClusterID: etcd.DefaultCDCClusterID,
				Owner:     map[string]struct{}{"22317526c4fc9a37": {}, "22317526c4fc9a38": {}},
				Captures: map[model.CaptureID]*model.CaptureInfo{"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
					ID:            "6bbc01c8-0605-4f86-a0f9-b3119109b225",
					AdvertiseAddr: "127.0.0.1:8300",
				}},
				Upstreams: map[model.UpstreamID]*model.UpstreamInfo{
					model.UpstreamID(12345): {},
				},
				Changefeeds: map[model.ChangeFeedID]*ChangefeedReactorState{
					model.DefaultChangeFeedID("test1"): {
						ClusterID: etcd.DefaultCDCClusterID,
						ID:        model.DefaultChangeFeedID("test1"),
						TaskPositions: map[model.CaptureID]*model.TaskPosition{
							"6bbc01c8-0605-4f86-a0f9-b3119109b225": {CheckPointTs: 421980719742451713, ResolvedTs: 421980720003809281},
						},
					},
					model.DefaultChangeFeedID("test2"): {
						ClusterID: etcd.DefaultCDCClusterID,
						ID:        model.DefaultChangeFeedID("test2"),
						TaskPositions: map[model.CaptureID]*model.TaskPosition{
							"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
								CheckPointTs: 421980719742451713,
								ResolvedTs:   421980720003809281,
							},
						},
					},
				},
			},
		},
		{ // testing remove changefeed
			updateKey: []string{
				etcd.DefaultClusterAndMetaPrefix +
					"/owner/22317526c4fc9a37",
				etcd.DefaultClusterAndMetaPrefix +
					"/owner/22317526c4fc9a38",
				etcd.DefaultClusterAndMetaPrefix +
					"/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test2",
				etcd.DefaultClusterAndMetaPrefix +
					"/owner/22317526c4fc9a37",
				etcd.DefaultClusterAndNamespacePrefix +
					"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test1",
				etcd.DefaultClusterAndMetaPrefix +
					"/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
			},
			updateValue: []string{
				`6bbc01c8-0605-4f86-a0f9-b3119109b225`,
				`55551111`,
				`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300"}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,
		"admin-job-type":0}`,
				`{"resolved-ts":421980720003809281,"checkpoint-ts":421980719742451713,
		"admin-job-type":0}`,
				``,
				``,
				``,
			},
			timeout: 6,
			expected: GlobalReactorState{
				ClusterID: etcd.DefaultCDCClusterID,
				Owner:     map[string]struct{}{"22317526c4fc9a38": {}},
				Captures:  map[model.CaptureID]*model.CaptureInfo{},
				Upstreams: map[model.UpstreamID]*model.UpstreamInfo{},
				Changefeeds: map[model.ChangeFeedID]*ChangefeedReactorState{
					model.DefaultChangeFeedID("test2"): {
						ClusterID: etcd.DefaultCDCClusterID,
						ID:        model.DefaultChangeFeedID("test2"),
						TaskPositions: map[model.CaptureID]*model.TaskPosition{
							"6bbc01c8-0605-4f86-a0f9-b3119109b225": {
								CheckPointTs: 421980719742451713,
								ResolvedTs:   421980720003809281,
							},
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		state := NewGlobalState(etcd.DefaultCDCClusterID, 10)
		for i, k := range tc.updateKey {
			value := []byte(tc.updateValue[i])
			if len(value) == 0 {
				value = nil
			}
			err := state.Update(util.NewEtcdKey(k), value, false)
			require.Nil(t, err)
		}
		time.Sleep(time.Duration(tc.timeout) * time.Second)
		state.UpdatePendingChange()
		require.True(t, cmp.Equal(state, &tc.expected, cmpopts.IgnoreUnexported(GlobalReactorState{}, ChangefeedReactorState{})),
			cmp.Diff(state, &tc.expected, cmpopts.IgnoreUnexported(GlobalReactorState{}, ChangefeedReactorState{})))
	}
}

func TestCaptureChangeHooks(t *testing.T) {
	t.Parallel()

	state := NewGlobalState(etcd.DefaultCDCClusterID, 10)

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

	err = state.Update(util.NewEtcdKey(
		etcd.CaptureInfoKeyPrefix(etcd.DefaultCDCClusterID)+"/capture-1"),
		captureInfoBytes, false)
	require.Nil(t, err)
	require.Eventually(t, func() bool {
		return callCount == 1
	}, time.Second*3, 10*time.Millisecond)

	err = state.Update(util.NewEtcdKey(
		etcd.CaptureInfoKeyPrefix(etcd.DefaultCDCClusterID)+"/capture-1"),
		nil /* delete */, false)
	require.Nil(t, err)
	require.Eventually(t, func() bool {
		state.UpdatePendingChange()
		return callCount == 2
	}, time.Second*10, 10*time.Millisecond)
}

func TestCheckChangefeedNormal(t *testing.T) {
	state := NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID("test1"))
	stateTester := NewReactorStateTester(t, state, nil)
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{SinkURI: "123", AdminJobType: model.AdminNone, Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{CheckpointTs: 1, AdminJobType: model.AdminNone}, true, nil
	})
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status.CheckpointTs, uint64(1))

	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		info.AdminJobType = model.AdminStop
		return info, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 2
		return status, true, nil
	})
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status.CheckpointTs, uint64(1))

	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 2
		return status, true, nil
	})
	state.CheckChangefeedNormal()
	stateTester.MustApplyPatches()
	require.Equal(t, state.Status.CheckpointTs, uint64(2))
}

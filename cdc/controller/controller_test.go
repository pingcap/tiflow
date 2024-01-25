// Copyright 2023 PingCAP, Inc.
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

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func createController4Test(ctx cdcContext.Context,
	t *testing.T) (*controllerImpl, *orchestrator.GlobalReactorState,
	*orchestrator.ReactorStateTester,
) {
	pdClient := &gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint, nil
		},
	}

	m := upstream.NewManager4Test(pdClient)
	o := NewController(m, &model.CaptureInfo{}, nil).(*controllerImpl)

	state := orchestrator.NewGlobalStateForTest(etcd.DefaultCDCClusterID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)

	// set captures
	cdcKey := etcd.CDCKey{
		ClusterID: state.ClusterID,
		Tp:        etcd.CDCKeyTypeCapture,
		CaptureID: ctx.GlobalVars().CaptureInfo.ID,
	}
	captureBytes, err := ctx.GlobalVars().CaptureInfo.Marshal()
	require.Nil(t, err)
	tester.MustUpdate(cdcKey.String(), captureBytes)
	return o, state, tester
}

func TestFixChangefeedState(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	controller4Test, state, tester := createController4Test(ctx, t)
	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	// Mismatched state and admin job.
	changefeedInfo := &model.ChangeFeedInfo{
		State:        model.StateNormal,
		AdminJobType: model.AdminStop,
		StartTs:      oracle.GoTimeToTS(time.Now()),
		Config:       config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		ClusterID:    state.ClusterID,
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	// For the first tick, we do a bootstrap, and it tries to fix the meta information.
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.NotContains(t, controller4Test.changefeeds, changefeedID)
	// Start tick normally.
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, controller4Test.changefeeds, changefeedID)
	// The meta information is fixed correctly.
	require.Equal(t, controller4Test.changefeeds[changefeedID].Info.State, model.StateStopped)
}

func TestCheckClusterVersion(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	controller4Test, state, tester := createController4Test(ctx, t)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

	tester.MustUpdate(fmt.Sprintf("%s/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		etcd.DefaultClusterAndMetaPrefix),
		[]byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225",
"address":"127.0.0.1:8300","version":"v6.0.0"}`))

	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	changefeedInfo := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		ClusterID:    state.ClusterID,
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))

	// check the tick is skipped and the changefeed will not be handled
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.NotContains(t, controller4Test.changefeeds, changefeedID)

	tester.MustUpdate(fmt.Sprintf("%s/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		etcd.DefaultClusterAndMetaPrefix,
	),
		[]byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300","version":"`+
			ctx.GlobalVars().CaptureInfo.Version+`"}`))

	// check the tick is not skipped and the changefeed will be handled normally
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, controller4Test.changefeeds, changefeedID)
}

func TestFixChangefeedSinkProtocol(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	controller4Test, state, tester := createController4Test(ctx, t)
	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	// Unknown protocol.
	changefeedInfo := &model.ChangeFeedInfo{
		State:          model.StateNormal,
		AdminJobType:   model.AdminStop,
		StartTs:        oracle.GoTimeToTS(time.Now()),
		CreatorVersion: "5.3.0",
		SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2?protocol=random",
		Config: &config.ReplicaConfig{
			Sink: &config.SinkConfig{Protocol: util.AddressOf(config.ProtocolDefault.String())},
		},
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		ClusterID:    state.ClusterID,
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	// For the first tick, we do a bootstrap, and it tries to fix the meta information.
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.NotContains(t, controller4Test.changefeeds, changefeedID)

	// Start tick normally.
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, controller4Test.changefeeds, changefeedID)
	// The meta information is fixed correctly.
	require.Equal(t, controller4Test.changefeeds[changefeedID].Info.SinkURI,
		"kafka://127.0.0.1:9092/ticdc-test2?protocol=open-protocol")
}

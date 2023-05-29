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

package v1

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestVerifyUpdateChangefeedConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	oldInfo := &model.ChangeFeedInfo{Config: config.GetDefaultReplicaConfig()}
	// test startTs > targetTs
	changefeedConfig := model.ChangefeedConfig{TargetTS: 20}
	oldInfo.StartTs = 40
	newInfo, err := VerifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	require.NotNil(t, err)
	require.Regexp(t, ".*can not update target-ts.*less than start-ts.*", err)
	require.Nil(t, newInfo)

	// test no change error
	changefeedConfig = model.ChangefeedConfig{SinkURI: "blackhole://"}
	oldInfo.SinkURI = "blackhole://"
	oldInfo.Config.Sink.TxnAtomicity = util.AddressOf(config.AtomicityLevel("none"))
	newInfo, err = VerifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	require.NotNil(t, err)
	require.Regexp(t, ".*changefeed config is the same with the old one.*", err)
	require.Nil(t, newInfo)

	// test verify success
	changefeedConfig = model.ChangefeedConfig{MounterWorkerNum: 32}
	newInfo, err = VerifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	require.Nil(t, err)
	require.NotNil(t, newInfo)
	require.NotEqual(t, 0, newInfo.Epoch)
}

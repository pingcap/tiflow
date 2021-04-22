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

package owner

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"

	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

type gcManager struct {
	pdClient pd.Client
	gcTTL    int64

	lastUpdatedTime time.Time
	lastSucceedTime time.Time
	lastSafePointTs uint64
}

func newGCManager(pdClient pd.Client) *gcManager {
	serverConfig := config.GetGlobalServerConfig()
	return &gcManager{
		pdClient: pdClient,
		gcTTL:    serverConfig.GcTTL,
	}
}

func (m *gcManager) updateGCSafePoint(ctx context.Context, state *model.GlobalReactorState) error {
	if time.Since(m.lastUpdatedTime) < gcSafepointUpdateInterval {
		return nil
	}
	m.lastUpdatedTime = time.Now()
	minCheckpointTs := uint64(math.MaxUint64)
	for _, cfState := range state.Changefeeds {
		if cfState.Info == nil {
			continue
		}
		switch cfState.Info.State {
		case model.StateNormal, model.StateStopped, model.StateError:
		default:
			continue
		}
		checkpointTs := cfState.Info.GetCheckpointTs(cfState.Status)
		if minCheckpointTs > checkpointTs {
			minCheckpointTs = checkpointTs
		}
	}

	actual, err := m.pdClient.UpdateServiceGCSafePoint(ctx, cdcServiceSafePointID, m.gcTTL, minCheckpointTs)
	if err != nil {
		log.Warn("updateGCSafePoint failed",
			zap.Uint64("safePointTs", minCheckpointTs),
			zap.Error(err))
		if time.Since(m.lastSucceedTime) >= time.Second*time.Duration(m.gcTTL) {
			return cerror.ErrUpdateServiceSafepointFailed.Wrap(err)
		}
		return nil
	}
	m.lastSafePointTs = actual
	m.lastSucceedTime = time.Now()
	return nil
}

func (m gcManager) GcSafePointTs() model.Ts {
	return m.lastSafePointTs
}

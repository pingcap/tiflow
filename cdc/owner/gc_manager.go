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
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

const (
	// cdcServiceSafePointID is the ID of CDC service in pd.UpdateServiceGCSafePoint.
	cdcServiceSafePointID = "ticdc"
	pdTimeUpdateInterval  = 10 * time.Minute
)

// gcSafepointUpdateInterval is the minimual interval that CDC can update gc safepoint
var gcSafepointUpdateInterval = 1 * time.Minute

type gcManager struct {
	gcTTL int64

	lastUpdatedTime   time.Time
	lastSucceededTime time.Time
	lastSafePointTs   uint64

	pdPhysicalTimeCache time.Time
	lastUpdatedPdTime   time.Time
}

func newGCManager() *gcManager {
	serverConfig := config.GetGlobalServerConfig()
	failpoint.Inject("InjectGcSafepointUpdateInterval", func(val failpoint.Value) {
		gcSafepointUpdateInterval = time.Duration(val.(int) * int(time.Millisecond))
	})
	return &gcManager{
		gcTTL: serverConfig.GcTTL,
	}
}

func (m *gcManager) updateGCSafePoint(ctx cdcContext.Context, state *model.GlobalReactorState) error {
	if time.Since(m.lastUpdatedTime) < gcSafepointUpdateInterval {
		return nil
	}
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
	if minCheckpointTs == math.MaxUint64 {
		return nil
	}
	m.lastUpdatedTime = time.Now()

	actual, err := ctx.GlobalVars().PDClient.UpdateServiceGCSafePoint(ctx, cdcServiceSafePointID, m.gcTTL, minCheckpointTs)
	if err != nil {
		log.Warn("updateGCSafePoint failed",
			zap.Uint64("safePointTs", minCheckpointTs),
			zap.Error(err))
		if time.Since(m.lastSucceededTime) >= time.Second*time.Duration(m.gcTTL) {
			return cerror.ErrUpdateServiceSafepointFailed.Wrap(err)
		}
		return nil
	}
	failpoint.Inject("InjectActualGCSafePoint", func(val failpoint.Value) {
		actual = uint64(val.(int))
	})
	if actual > minCheckpointTs {
		log.Warn("update gc safe point failed, the gc safe point is larger than checkpointTs", zap.Uint64("actual", actual), zap.Uint64("checkpointTs", minCheckpointTs))
	}
	m.lastSafePointTs = actual
	m.lastSucceededTime = time.Now()
	return nil
}

func (m *gcManager) currentTimeFromPDCached(ctx cdcContext.Context) (time.Time, error) {
	if time.Since(m.lastUpdatedPdTime) <= pdTimeUpdateInterval {
		return m.pdPhysicalTimeCache, nil
	}
	physical, logical, err := ctx.GlobalVars().PDClient.GetTS(ctx)
	if err != nil {
		return time.Now(), errors.Trace(err)
	}
	m.pdPhysicalTimeCache = oracle.GetTimeFromTS(oracle.ComposeTS(physical, logical))
	m.lastUpdatedPdTime = time.Now()
	return m.pdPhysicalTimeCache, nil
}

func (m *gcManager) CheckStaleCheckpointTs(ctx cdcContext.Context, checkpointTs model.Ts) error {
	pdTime, err := m.currentTimeFromPDCached(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if checkpointTs < m.lastSafePointTs || pdTime.Sub(oracle.GetTimeFromTS(checkpointTs)) > time.Duration(m.gcTTL)*time.Second {
		return cerror.ErrSnapshotLostByGC.GenWithStackByArgs(checkpointTs, m.lastSafePointTs)
	}
	return nil
}

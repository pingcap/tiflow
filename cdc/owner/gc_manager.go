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
<<<<<<< HEAD:cdc/owner/gc_manager.go
	// cdcServiceSafePointID is the ID of CDC service in pd.UpdateServiceGCSafePoint.
	cdcServiceSafePointID = "ticdc"
	pdTimeUpdateInterval  = 10 * time.Minute
=======
	// CDCServiceSafePointID is the ID of CDC service in pd.UpdateServiceGCSafePoint.
	CDCServiceSafePointID = "ticdc"
>>>>>>> c91af794e (*: fix changefeed checkpoint lag negative value error (#3013)):pkg/txnutil/gc/gc_manager.go
)

// gcSafepointUpdateInterval is the minimum interval that CDC can update gc safepoint
var gcSafepointUpdateInterval = 1 * time.Minute

<<<<<<< HEAD:cdc/owner/gc_manager.go
// GcManager is an interface for gc manager
type GcManager interface {
	updateGCSafePoint(ctx cdcContext.Context, state *model.GlobalReactorState) error
	currentTimeFromPDCached(ctx cdcContext.Context) (time.Time, error)
	checkStaleCheckpointTs(ctx cdcContext.Context, changefeedID model.ChangeFeedID, checkpointTs model.Ts) error
}

type gcManager struct {
	gcTTL int64
=======
// Manager is an interface for gc manager
type Manager interface {
	// TryUpdateGCSafePoint tries to update TiCDC service GC safepoint.
	// Manager may skip update when it thinks it is too frequent.
	// Set `forceUpdate` to force Manager update.
	TryUpdateGCSafePoint(ctx context.Context, checkpointTs model.Ts, forceUpdate bool) error
	CheckStaleCheckpointTs(ctx context.Context, changefeedID model.ChangeFeedID, checkpointTs model.Ts) error
}

type gcManager struct {
	pdClient pd.Client
	gcTTL    int64
>>>>>>> c91af794e (*: fix changefeed checkpoint lag negative value error (#3013)):pkg/txnutil/gc/gc_manager.go

	lastUpdatedTime   time.Time
	lastSucceededTime time.Time
	lastSafePointTs   uint64
	isTiCDCBlockGC    bool
}

<<<<<<< HEAD:cdc/owner/gc_manager.go
func newGCManager() *gcManager {
=======
// NewManager creates a new Manager.
func NewManager(pdClient pd.Client) Manager {
>>>>>>> c91af794e (*: fix changefeed checkpoint lag negative value error (#3013)):pkg/txnutil/gc/gc_manager.go
	serverConfig := config.GetGlobalServerConfig()
	failpoint.Inject("InjectGcSafepointUpdateInterval", func(val failpoint.Value) {
		gcSafepointUpdateInterval = time.Duration(val.(int) * int(time.Millisecond))
	})
	return &gcManager{
<<<<<<< HEAD:cdc/owner/gc_manager.go
=======
		pdClient:          pdClient,
>>>>>>> c91af794e (*: fix changefeed checkpoint lag negative value error (#3013)):pkg/txnutil/gc/gc_manager.go
		lastSucceededTime: time.Now(),
		gcTTL:             serverConfig.GcTTL,
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
	if actual == minCheckpointTs {
		log.Info("update gc safe point success", zap.Uint64("gcSafePointTs", minCheckpointTs))
	}
	if actual > minCheckpointTs {
		log.Warn("update gc safe point failed, the gc safe point is larger than checkpointTs", zap.Uint64("actual", actual), zap.Uint64("checkpointTs", minCheckpointTs))
	}
	// if the min checkpoint ts is equal to the current gc safe point,
	// it means that the service gc safe point set by TiCDC is the min service gc safe point
	m.isTiCDCBlockGC = actual == minCheckpointTs
	m.lastSafePointTs = actual
	m.lastSucceededTime = time.Now()
	return nil
}

<<<<<<< HEAD:cdc/owner/gc_manager.go
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

func (m *gcManager) checkStaleCheckpointTs(
	ctx cdcContext.Context, changefeedID model.ChangeFeedID, checkpointTs model.Ts,
) error {
	gcSafepointUpperBound := checkpointTs - 1
	if m.isTiCDCBlockGC {
		pdTime, err := m.currentTimeFromPDCached(ctx)
=======
func (m *gcManager) CheckStaleCheckpointTs(
	ctx context.Context, changefeedID model.ChangeFeedID, checkpointTs model.Ts,
) error {
	gcSafepointUpperBound := checkpointTs - 1
	if m.isTiCDCBlockGC {
		cctx, ok := ctx.(cdcContext.Context)
		if !ok {
			return cerror.ErrOwnerUnknown.GenWithStack("ctx not an cdcContext.Context, it should be")
		}
		pdTime, err := cctx.GlobalVars().TimeAcquirer.CurrentTimeFromCached()
		// TODO: should we return err here, or just log it?
>>>>>>> c91af794e (*: fix changefeed checkpoint lag negative value error (#3013)):pkg/txnutil/gc/gc_manager.go
		if err != nil {
			return errors.Trace(err)
		}
		if pdTime.Sub(oracle.GetTimeFromTS(gcSafepointUpperBound)) > time.Duration(m.gcTTL)*time.Second {
			return cerror.ErrGCTTLExceeded.GenWithStackByArgs(checkpointTs, changefeedID)
		}
	} else {
		// if `isTiCDCBlockGC` is false, it means there is another service gc point less than the min checkpoint ts.
		if gcSafepointUpperBound < m.lastSafePointTs {
			return cerror.ErrSnapshotLostByGC.GenWithStackByArgs(checkpointTs, m.lastSafePointTs)
		}
	}
	return nil
}

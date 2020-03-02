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

package kv

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// regionActive records the activeness of a region
type regionActive struct {
	sync.RWMutex
	regionID      uint64
	threshold     int64
	lastKv        time.Time
	lastResolve   time.Time
	lastResolveTs uint64
	kvActive      bool
	resolveActive bool
}

func newRegionActive(regionID uint64, threshold int64) *regionActive {
	now := time.Now()

	return &regionActive{
		regionID:      regionID,
		lastKv:        now.Add(time.Second * time.Duration(-threshold)),
		lastResolve:   now.Add(time.Second * time.Duration(-threshold)),
		threshold:     threshold,
		kvActive:      false,
		resolveActive: false,
	}
}

func (aw *regionActive) SetKv() {
	aw.Lock()
	aw.lastKv = time.Now()
	aw.Unlock()
}

func (aw *regionActive) SetResolve(resolvedTs uint64) {
	aw.Lock()
	aw.lastResolve = time.Now()
	if aw.lastResolveTs == resolvedTs {
		log.Warn("resolve ts not forward", zap.Uint64("region", aw.regionID), zap.Uint64("ts", aw.lastResolveTs))
	}
	aw.lastResolveTs = resolvedTs
	aw.Unlock()
}

func (aw *regionActive) IsKvActive() bool {
	aw.RLock()
	defer aw.RUnlock()
	return aw.kvActive
}

func (aw *regionActive) IsResolveActive() bool {
	aw.RLock()
	defer aw.RUnlock()
	return aw.resolveActive
}

func (aw *regionActive) Check(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	captureID := util.GetValueFromCtx(ctx, util.CtxKeyCaptureID)
	changefeedID := util.GetValueFromCtx(ctx, util.CtxKeyChangefeedID)
	tableID := util.GetValueFromCtx(ctx, util.CtxKeyTableID)

	aw.RLock()
	defer aw.RUnlock()
	now := time.Now()
	kv := now.Unix() - aw.lastKv.Unix()
	resolve := now.Unix() - aw.lastResolve.Unix()
	if kv >= aw.threshold && aw.kvActive {
		aw.kvActive = false
		regionActiveKvCountGauge.WithLabelValues(captureID, changefeedID, tableID).Dec()
	}
	if kv < aw.threshold && !aw.kvActive {
		aw.kvActive = true
		regionActiveKvCountGauge.WithLabelValues(captureID, changefeedID, tableID).Inc()
	}
	if resolve >= aw.threshold && aw.resolveActive {
		log.Warn("region becomes resolve inactive", zap.Uint64("region", aw.regionID), zap.Int64("resolve duration", resolve))
		aw.resolveActive = false
		regionActiveResolveCountGauge.WithLabelValues(captureID, changefeedID, tableID).Dec()
	}
	if resolve < aw.threshold && !aw.resolveActive {
		aw.resolveActive = true
		regionActiveResolveCountGauge.WithLabelValues(captureID, changefeedID, tableID).Inc()
	}
}

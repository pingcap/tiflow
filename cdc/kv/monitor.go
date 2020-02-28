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
	kvActive      bool
	resolveActive bool
}

func newRegionActive(regionID uint64, threshold int64) *regionActive {
	now := time.Now()
	return &regionActive{
		regionID:      regionID,
		lastKv:        now,
		lastResolve:   now,
		threshold:     threshold,
		kvActive:      true,
		resolveActive: true,
	}
}

func (aw *regionActive) SetKv() {
	aw.Lock()
	aw.lastKv = time.Now()
	aw.Unlock()
}

func (aw *regionActive) SetResolve() {
	aw.Lock()
	aw.lastResolve = time.Now()
	aw.Unlock()
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
	if resolve >= aw.threshold {
		log.Warn("region resolve not active", zap.Uint64("region", aw.regionID), zap.Int64("resolve duration", resolve))
		if aw.resolveActive {
			aw.resolveActive = false
			regionActiveResolveCountGauge.WithLabelValues(captureID, changefeedID, tableID).Dec()
		}
	} else if !aw.resolveActive {
		aw.resolveActive = true
		regionActiveResolveCountGauge.WithLabelValues(captureID, changefeedID, tableID).Inc()
	}
}

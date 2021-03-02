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

package replication

import (
	"context"
	"time"

	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	// CDCServiceSafePointID is the ID of CDC service in pd.UpdateServiceGCSafePoint.
	cdcServiceSafePointID = "ticdc"
	// GCSafepointUpdateInterval is the minimual interval that CDC can update gc safepoint
	gcSafepointUpdateInterval = time.Duration(2 * time.Second)
)

type gcManager struct {
	pdClient pd.Client
	ttl      int64

	lastFlushedTime time.Time
	lastSafePointTs uint64
}

func newGCManager(pdClient pd.Client, ttl int64) *gcManager {
	return &gcManager{
		pdClient: pdClient,
		ttl:      ttl,
	}
}

func (m *gcManager) updateGCSafePoint(ctx context.Context, safePointTs uint64) (actual uint64, err error) {
	if time.Since(m.lastFlushedTime) > gcSafepointUpdateInterval || m.lastSafePointTs == 0 {
		startTime := time.Now()
		actual, err = m.pdClient.UpdateServiceGCSafePoint(ctx, cdcServiceSafePointID, m.ttl, safePointTs)
		if err != nil {
			log.Warn("updateGCSafePoint failed",
				zap.Uint64("safePointTs", safePointTs),
				zap.Error(err))

			if time.Since(startTime) < time.Duration(m.ttl)*time.Second {
				actual = m.lastSafePointTs
				err = nil
				return
			}

			return
		}
		m.lastFlushedTime = startTime
		m.lastSafePointTs = safePointTs
		return
	}

	actual = m.lastSafePointTs
	return
}

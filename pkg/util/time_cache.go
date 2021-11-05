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

package util

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"

	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

const pdTimeUpdateInterval = 200 * time.Millisecond

// PDTimeCache cache time get from PD client
type PDTimeCache struct {
	pdClient            pd.Client
	pdPhysicalTimeCache time.Time
	lastUpdatedPdTime   time.Time
	mu                  sync.RWMutex
	stop                chan struct{}
	err                 error
}

// NewPDTimeCache return a new PDTimeCache
func NewPDTimeCache(pdClient pd.Client) *PDTimeCache {
	return &PDTimeCache{
		pdClient: pdClient,
	}
}

func (m *PDTimeCache) Run(ctx context.Context) {
	ticker := time.NewTicker(pdTimeUpdateInterval)
	for {
		select {
		case <-ctx.Done():
			log.Info("PDTimeCache exit")
			return
		case <-m.stop:
			log.Info("PDTimeCache stop")
			return
		case <-ticker.C:
			err := retry.Do(ctx, func() error {
				physical, _, err := m.pdClient.GetTS(ctx)
				if err != nil {
					log.Info("get owner failed, retry later", zap.Error(err))
					return err
				}
				m.mu.Lock()
				m.pdPhysicalTimeCache = oracle.GetTimeFromTS(oracle.ComposeTS(physical, 0))
				m.lastUpdatedPdTime = time.Now()
				m.err = nil
				m.mu.Unlock()
				return nil
			}, retry.WithBackoffBaseDelay(300), retry.WithMaxTries(10))
			if err != nil {
				log.Warn("fail to get time from pd, will use local time as pd time")
				m.mu.Lock()
				m.err = err
				m.mu.Unlock()
			}
		}
	}
}

// CurrentTimeFromPDCached return current time from pd cache
func (m *PDTimeCache) CurrentTimeFromPDCached() (time.Time, error) {
	m.mu.RLock()
	err := m.err
	cacheTime := m.pdPhysicalTimeCache
	m.mu.RUnlock()
	return cacheTime, err
}

func (m *PDTimeCache) Stop() {
	m.stop <- struct{}{}
}

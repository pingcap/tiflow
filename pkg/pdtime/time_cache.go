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

package pdtime

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"

	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

const pdTimeUpdateInterval = 200 * time.Millisecond

// TimeAcquirer cache time get from PD periodically and cache it
type TimeAcquirer struct {
	pdClient  pd.Client
	timeCache time.Time
	mu        sync.RWMutex
	stop      chan struct{}
	err       error
}

// NewTimeAcquirer return a new TimeAcquirer
func NewTimeAcquirer(pdClient pd.Client) *TimeAcquirer {
	return &TimeAcquirer{
		pdClient: pdClient,
		stop:     make(chan struct{}),
	}
}

// Run will get time from pd periodically to cache in pdPhysicalTimeCache
func (c *TimeAcquirer) Run(ctx context.Context) {
	ticker := time.NewTicker(pdTimeUpdateInterval)
	for {
		select {
		case <-ctx.Done():
			log.Info("TimeAcquirer exit")
			return
		case <-c.stop:
			log.Info("TimeAcquirer eixt")
			return
		case <-ticker.C:
			err := retry.Do(ctx, func() error {
				physical, _, err := c.pdClient.GetTS(ctx)
				if err != nil {
					log.Info("get time from pd failed, retry later", zap.Error(err))
					return err
				}
				c.mu.Lock()
				c.timeCache = oracle.GetTimeFromTS(oracle.ComposeTS(physical, 0))
				c.err = nil
				c.mu.Unlock()
				return nil
			}, retry.WithBackoffBaseDelay(200), retry.WithMaxTries(10))
			if err != nil {
				log.Warn("get time from pd failed, will use local time as pd time")
				c.mu.Lock()
				c.timeCache = time.Now()
				c.err = err
				c.mu.Unlock()
			}
		}
	}
}

// CurrentTimeFromCached return current time from pd cache
func (c *TimeAcquirer) CurrentTimeFromCached() (time.Time, error) {
	c.mu.RLock()
	err := c.err
	cacheTime := c.timeCache
	c.mu.RUnlock()
	return cacheTime, errors.Trace(err)
}

func (c *TimeAcquirer) Stop() {
	c.stop <- struct{}{}
}

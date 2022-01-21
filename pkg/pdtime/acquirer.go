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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/pkg/retry"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const pdTimeUpdateInterval = 200 * time.Millisecond

// TimeAcquirer cache time get from PD periodically
type TimeAcquirer interface {
	// Run run the TimeAcquirer
	Run(ctx context.Context)
	// CurrentTimeFromCached returns current time from cache
	CurrentTimeFromCached() (time.Time, error)
	// Stop stops the TimeAcquirer
	Stop()
}

// TimeAcquirerImpl cache time get from PD periodically and cache it
type TimeAcquirerImpl struct {
	pdClient  pd.Client
	timeCache time.Time
	mu        sync.RWMutex
	cancel    context.CancelFunc
	err       error
}

// NewTimeAcquirer return a new TimeAcquirer
func NewTimeAcquirer(pdClient pd.Client) TimeAcquirer {
	return &TimeAcquirerImpl{
		pdClient: pdClient,
	}
}

// Run will get time from pd periodically to cache in pdPhysicalTimeCache
func (c *TimeAcquirerImpl) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	ticker := time.NewTicker(pdTimeUpdateInterval)
	for {
		select {
		// c.Stop() was called or parent ctx was canceled
		case <-ctx.Done():
			log.Info("TimeAcquirer exit")
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
func (c *TimeAcquirerImpl) CurrentTimeFromCached() (time.Time, error) {
	c.mu.RLock()
	err := c.err
	cacheTime := c.timeCache
	c.mu.RUnlock()
	return cacheTime, errors.Trace(err)
}

// Stop stop TimeAcquirer
func (c *TimeAcquirerImpl) Stop() {
	c.cancel()
}

// TimeAcquirer4Test only for test
type TimeAcquirer4Test struct{}

// NewTimeAcquirer4Test return a TimeAcquirer for test
func NewTimeAcquirer4Test() TimeAcquirer {
	return &TimeAcquirer4Test{}
}

// CurrentTimeFromCached return current time
func (c *TimeAcquirer4Test) CurrentTimeFromCached() (time.Time, error) {
	return time.Now(), nil
}

// Run implements TimeAcquirer
func (c *TimeAcquirer4Test) Run(ctx context.Context) {
}

// Stop implements TimeAcquirer
func (c *TimeAcquirer4Test) Stop() {
}

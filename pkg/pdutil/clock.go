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

package pdutil

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const pdTimeUpdateInterval = 200 * time.Millisecond

// Clock is a time source of PD cluster.
type Clock interface {
	// CurrentTime returns current time from PD.
	CurrentTime() (time.Time, error)
	Run(ctx context.Context)
	Stop()
}

// PDClock cache time get from PD periodically and cache it
type PDClock struct {
	pdClient pd.Client
	mu       struct {
		sync.RWMutex
		timeCache time.Time
		err       error
	}
	cancel context.CancelFunc
	stopCh chan struct{}
}

// NewClock return a new PDClock
func NewClock(ctx context.Context, pdClient pd.Client) (*PDClock, error) {
	ret := &PDClock{
		pdClient: pdClient,
		stopCh:   make(chan struct{}, 1),
	}
	physical, _, err := pdClient.GetTS(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.mu.timeCache = oracle.GetTimeFromTS(oracle.ComposeTS(physical, 0))
	return ret, nil
}

// Run will get time from pd periodically to cache in timeCache
func (c *PDClock) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	ticker := time.NewTicker(pdTimeUpdateInterval)
	defer func() { c.stopCh <- struct{}{} }()
	for {
		select {
		// c.Stop() was called or parent ctx was canceled
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := retry.Do(ctx, func() error {
				physical, _, err := c.pdClient.GetTS(ctx)
				if err != nil {
					log.Info("get time from pd failed, retry later", zap.Error(err))
					return err
				}
				c.mu.Lock()
				c.mu.timeCache = oracle.GetTimeFromTS(oracle.ComposeTS(physical, 0))
				c.mu.err = nil
				c.mu.Unlock()
				return nil
			}, retry.WithBackoffBaseDelay(200), retry.WithMaxTries(10))
			if err != nil {
				log.Warn("get time from pd failed, will use local time as pd time")
				c.mu.Lock()
				c.mu.timeCache = time.Now()
				c.mu.err = err
				c.mu.Unlock()
			}
		}
	}
}

// CurrentTime returns current time from timeCache
func (c *PDClock) CurrentTime() (time.Time, error) {
	c.mu.RLock()
	err := c.mu.err
	cacheTime := c.mu.timeCache
	c.mu.RUnlock()
	return cacheTime, errors.Trace(err)
}

// Stop PDClock.
func (c *PDClock) Stop() {
	c.cancel()
	<-c.stopCh
}

type clock4Test struct{}

// NewClock4Test return a new clock for test.
func NewClock4Test() Clock {
	return &clock4Test{}
}

func (c *clock4Test) CurrentTime() (time.Time, error) {
	return time.Now(), nil
}

func (c *clock4Test) Run(ctx context.Context) {
}

func (c *clock4Test) Stop() {
}

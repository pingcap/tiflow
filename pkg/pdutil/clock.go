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
	pclock "github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const pdTimeUpdateInterval = 10 * time.Millisecond

// Clock is a time source of PD cluster.
type Clock interface {
	// CurrentTime returns approximate current time from pd.
	CurrentTime() (time.Time, error)
	Run(ctx context.Context)
	Stop()
}

// clock cache time get from PD periodically and cache it
type clock struct {
	pdClient pd.Client
	mu       struct {
		sync.RWMutex
		// The time encoded in PD ts.
		tsEventTime time.Time
		// The time we receive PD ts.
		tsProcessingTime time.Time
		err              error
	}
	updateInterval time.Duration
	cancel         context.CancelFunc
	stopCh         chan struct{}
}

// NewClock return a new clock
func NewClock(ctx context.Context, pdClient pd.Client) (*clock, error) {
	ret := &clock{
		pdClient:       pdClient,
		stopCh:         make(chan struct{}, 1),
		updateInterval: pdTimeUpdateInterval,
	}
	physical, _, err := pdClient.GetTS(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.mu.tsEventTime = oracle.GetTimeFromTS(oracle.ComposeTS(physical, 0))
	ret.mu.tsProcessingTime = time.Now()
	return ret, nil
}

// Run gets time from pd periodically.
func (c *clock) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()
	ticker := time.NewTicker(c.updateInterval)
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
				c.mu.tsEventTime = oracle.GetTimeFromTS(oracle.ComposeTS(physical, 0))
				c.mu.tsProcessingTime = time.Now()
				c.mu.err = nil
				c.mu.Unlock()
				return nil
			}, retry.WithBackoffBaseDelay(200), retry.WithMaxTries(10))
			if err != nil {
				log.Warn("get time from pd failed, will use local time as pd time")
				c.mu.Lock()
				now := time.Now()
				c.mu.tsEventTime = now
				c.mu.tsProcessingTime = now
				c.mu.err = err
				c.mu.Unlock()
			}
		}
	}
}

// CurrentTime returns approximate current time from pd.
func (c *clock) CurrentTime() (time.Time, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tsEventTime := c.mu.tsEventTime
	current := tsEventTime.Add(time.Since(c.mu.tsProcessingTime))
	return current, errors.Trace(c.mu.err)
}

// Stop clock.
func (c *clock) Stop() {
	c.mu.Lock()
	c.cancel()
	c.mu.Unlock()
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

type monotonicClock struct {
	clock pclock.Clock
}

// NewMonotonicClock return a new monotonic clock.
func NewMonotonicClock(pClock pclock.Clock) Clock {
	return &monotonicClock{
		clock: pClock,
	}
}

func (c *monotonicClock) CurrentTime() (time.Time, error) {
	return c.clock.Now(), nil
}

func (c *monotonicClock) Run(ctx context.Context) {
}

func (c *monotonicClock) Stop() {
}

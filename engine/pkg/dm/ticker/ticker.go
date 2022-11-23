// Copyright 2022 PingCAP, Inc.
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

package ticker

import (
	"context"
	"sync"
	"time"
)

// Ticker interface
type Ticker interface {
	TickImpl(ctx context.Context) error
}

// DefaultTicker defines the template method to run periodically.
type DefaultTicker struct {
	Ticker

	mu            sync.RWMutex
	lastCheckTime time.Time
	nextCheckTime time.Time

	normalInterval time.Duration
	errorInterval  time.Duration
}

// NewDefaultTicker creates a DefaultTicker instance
func NewDefaultTicker(normalInterval, errorInterval time.Duration) *DefaultTicker {
	defaultTicker := &DefaultTicker{
		normalInterval: normalInterval,
		errorInterval:  errorInterval,
	}
	defaultTicker.SetNextCheckTime(time.Now())
	return defaultTicker
}

// SetNextCheckTime sets the next check time if ticker has checked or the given
// check time hits (lastCheckTime, nextCheckTime)
func (s *DefaultTicker) SetNextCheckTime(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastCheckTime.Equal(s.nextCheckTime) || (t.After(s.lastCheckTime) && t.Before(s.nextCheckTime)) {
		s.nextCheckTime = t
	}
}

func (s *DefaultTicker) getNextCheckTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextCheckTime
}

func (s *DefaultTicker) advanceCheckTime() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.lastCheckTime = now
	s.nextCheckTime = now
}

// DoTick checks whether needs to check and calls TickImpl if needed.
func (s *DefaultTicker) DoTick(ctx context.Context) {
	if time.Now().Before(s.getNextCheckTime()) {
		return
	}
	s.advanceCheckTime()

	if err := s.TickImpl(ctx); err != nil {
		// TODO: add backoff strategy
		s.SetNextCheckTime(time.Now().Add(s.errorInterval))
	} else {
		s.SetNextCheckTime(time.Now().Add(s.normalInterval))
	}
}

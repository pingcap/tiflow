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

package jobop

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"go.uber.org/zap"
)

type backoffEventType int32

const (
	backoffOnline backoffEventType = iota + 1
	backoffOffline
)

type backoffEvent struct {
	tp backoffEventType
	ts time.Time
}

// NewJobBackoff creates a new job backoff
func NewJobBackoff(jobID string, clocker clock.Clock, config *BackoffConfig) *JobBackoff {
	errBackoff := backoff.NewExponentialBackOff()
	errBackoff.InitialInterval = config.InitialInterval
	errBackoff.MaxInterval = config.MaxInterval
	errBackoff.Multiplier = config.Multiplier
	// MaxElapsedTime=0 means the backoff never stops, since there is other ways
	// to stop backoff, including continuously failure check, cancel job.
	errBackoff.MaxElapsedTime = 0
	errBackoff.Reset()

	return &JobBackoff{
		jobID:      jobID,
		clocker:    clocker,
		config:     config,
		errBackoff: errBackoff,
	}
}

// JobBackoff is a job backoff manager, it recoreds job online and offline events
// and determines whether a job can be re-created based on backoff mechanism.
// The backoff stragegy is as following
//   - Each time a fail event arrives, the backoff time will be move forward by
//     nextBackoff.
//   - If a job is success for more than `resetInterval`, the backoff history will
//     be cleared, and backoff time will be re-calculated.
type JobBackoff struct {
	jobID   string
	clocker clock.Clock
	config  *BackoffConfig

	events          []backoffEvent
	errBackoff      *backoff.ExponentialBackOff
	backoffInterval time.Duration
}

// Terminate returns whether job should be terminated.
// It happens when job fails continuously for more than max try times.
func (b *JobBackoff) Terminate() bool {
	if len(b.events) < b.config.MaxTryTime {
		return false
	}
	failCount := 0
	for _, event := range b.events {
		if event.tp == backoffOffline {
			failCount++
		}
	}
	return failCount >= b.config.MaxTryTime
}

// Allow returns whether new request(create job) is allowd
func (b *JobBackoff) Allow() bool {
	var lastErrorTime time.Time
	for i := len(b.events) - 1; i >= 0; i-- {
		event := b.events[i]
		if event.tp == backoffOffline {
			lastErrorTime = event.ts
			break
		}
	}
	return b.clocker.Since(lastErrorTime) >= b.backoffInterval
}

// Success is called when a success event happens
func (b *JobBackoff) Success() {
	event := backoffEvent{
		tp: backoffOnline,
		ts: b.clocker.Now(),
	}
	b.addEvent(event)
}

// Fail is called when a failure event happens
func (b *JobBackoff) Fail() {
	event := backoffEvent{
		tp: backoffOffline,
		ts: b.clocker.Now(),
	}
	b.addEvent(event)
	b.nextBackoff()
}

// addEvent appends new backoff event into backoffer
func (b *JobBackoff) addEvent(event backoffEvent) {
	// The last event is online and it is earlier than `resetInterval`,
	// reset the backoff
	if len(b.events) > 0 {
		lastEvent := b.events[len(b.events)-1]
		if lastEvent.tp == backoffOnline &&
			b.clocker.Since(lastEvent.ts) >= b.config.ResetInterval {
			b.events = make([]backoffEvent, 0)
			b.resetErrBackoff()
		}
	}
	b.events = append(b.events, event)
}

func (b *JobBackoff) resetErrBackoff() {
	b.errBackoff.Reset()
	b.backoffInterval = 0
}

func (b *JobBackoff) nextBackoff() {
	oldInterval := b.backoffInterval
	b.backoffInterval = b.errBackoff.NextBackOff()
	log.Info("job backoff interval is changed",
		zap.String("job-id", b.jobID),
		zap.Duration("old-interval", oldInterval),
		zap.Duration("new-interval", b.backoffInterval),
	)
}

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

const (
	defaultBackoffInitInterval = 10 * time.Second
	defaultBackoffMaxInterval  = 5 * time.Minute
	defaultBackoffMultiplier   = 2.0
	// If a job can keep running for more than 10 minutes, it won't be backoff
	// If a job keeps failing, the max back interval is 5 minutes, and 10 minutes
	// can keep at least one failed record.
	defaultBackoffRestInterval = 2 * defaultBackoffMaxInterval
)

type backoffOpts struct {
	clocker         clock.Clock
	resetInterval   time.Duration
	initialInterval time.Duration
	maxInterval     time.Duration
	multiplier      float64
}

// BackoffOption is used to set options to job backoff
type BackoffOption func(*backoffOpts)

// WithResetInterval sets resetInterval to backoffOpts
func WithResetInterval(interval time.Duration) BackoffOption {
	return func(opts *backoffOpts) {
		opts.resetInterval = interval
	}
}

// WithInitialInterval sets initialInterval to backoffOpts
func WithInitialInterval(interval time.Duration) BackoffOption {
	return func(opts *backoffOpts) {
		opts.initialInterval = interval
	}
}

// WithMaxInterval sets maxInterval to backoffOpts
func WithMaxInterval(interval time.Duration) BackoffOption {
	return func(opts *backoffOpts) {
		opts.maxInterval = interval
	}
}

// WithMultiplier sets multiplier to backoffOpts
func WithMultiplier(multiplier float64) BackoffOption {
	return func(opts *backoffOpts) {
		opts.multiplier = multiplier
	}
}

// WithClocker sets clocker to backoffOpts
func WithClocker(clocker clock.Clock) BackoffOption {
	return func(opts *backoffOpts) {
		opts.clocker = clocker
	}
}

type backoffEvent struct {
	tp backoffEventType
	ts time.Time
}

// NewJobBackoff creates a new job backoff
func NewJobBackoff(jobID string, options ...BackoffOption) *JobBackoff {
	opts := &backoffOpts{
		resetInterval:   defaultBackoffRestInterval,
		initialInterval: defaultBackoffInitInterval,
		maxInterval:     defaultBackoffMaxInterval,
		multiplier:      defaultBackoffMultiplier,
		clocker:         clock.New(),
	}
	for _, option := range options {
		option(opts)
	}

	errBackoff := backoff.NewExponentialBackOff()
	errBackoff.InitialInterval = opts.initialInterval
	errBackoff.MaxInterval = opts.maxInterval
	errBackoff.Multiplier = opts.multiplier
	errBackoff.Reset()

	return &JobBackoff{
		jobID:      jobID,
		opts:       opts,
		errBackoff: errBackoff,
	}
}

// JobBackoff is a job backoff manager, it recoreds job online and offline events
// and determines whether a job can be re-created based on backoff mechanism.
// The backoff stragegy is as following
// - Each time a fail event arrives, the backoff time will be move forward by
//   nextBackoff.
// - If a job is success for more than `resetInterval`, the backoff history will
//   be cleared, and backoff time will be re-calculated.
type JobBackoff struct {
	jobID           string
	events          []backoffEvent
	opts            *backoffOpts
	errBackoff      *backoff.ExponentialBackOff
	backoffInterval time.Duration
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
	return b.opts.clocker.Since(lastErrorTime) >= b.backoffInterval
}

// Success is called when a success event happens
func (b *JobBackoff) Success() {
	event := backoffEvent{
		tp: backoffOnline,
		ts: b.opts.clocker.Now(),
	}
	b.addEvent(event)
}

// Fail is called when a failure event happens
func (b *JobBackoff) Fail() {
	event := backoffEvent{
		tp: backoffOffline,
		ts: b.opts.clocker.Now(),
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
			b.opts.clocker.Since(lastEvent.ts) >= b.opts.resetInterval {
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

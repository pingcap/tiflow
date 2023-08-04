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

package retry

import (
	"math"
	"time"
)

const (
	// defaultBackoffBaseInMs is the initial duration, in Millisecond
	defaultBackoffBaseInMs = 10.0
	// defaultBackoffCapInMs is the max amount of duration, in Millisecond
	defaultBackoffCapInMs   = 100.0
	defaultMaxTries         = math.MaxUint64
	defaultMaxRetryDuration = time.Duration(0)
)

// Option ...
type Option func(*retryOptions)

// IsRetryable checks the error is safe or worth to retry, eg. "context.Canceled" better not retry
type IsRetryable func(error) bool

type retryOptions struct {
	totalRetryDuration time.Duration
	maxTries           uint64
	backoffBaseInMs    float64
	backoffCapInMs     float64
	isRetryable        IsRetryable
}

func newRetryOptions() *retryOptions {
	return &retryOptions{
		totalRetryDuration: defaultMaxRetryDuration,
		maxTries:           defaultMaxTries,
		backoffBaseInMs:    defaultBackoffBaseInMs,
		backoffCapInMs:     defaultBackoffCapInMs,
		isRetryable:        func(err error) bool { return true },
	}
}

// WithBackoffBaseDelay configures the initial delay, if delayInMs <= 0 "defaultBackoffBaseInMs" will be used
func WithBackoffBaseDelay(delayInMs int64) Option {
	return func(o *retryOptions) {
		if delayInMs > 0 {
			o.backoffBaseInMs = float64(delayInMs)
		}
	}
}

// WithBackoffMaxDelay configures the maximum delay, if delayInMs <= 0 "defaultBackoffCapInMs" will be used
func WithBackoffMaxDelay(delayInMs int64) Option {
	return func(o *retryOptions) {
		if delayInMs > 0 {
			o.backoffCapInMs = float64(delayInMs)
		}
	}
}

// WithMaxTries configures maximum tries, if tries is 0, 1 will be used
func WithMaxTries(tries uint64) Option {
	return func(o *retryOptions) {
		if tries == 0 {
			tries = 1
		}
		o.maxTries = tries
	}
}

// WithTotalRetryDuration configures the total retry duration.
func WithTotalRetryDuration(retryDuration time.Duration) Option {
	return func(o *retryOptions) {
		o.totalRetryDuration = retryDuration
	}
}

// WithIsRetryableErr configures the error should retry or not, if not set, retry by default
func WithIsRetryableErr(f IsRetryable) Option {
	return func(o *retryOptions) {
		if f != nil {
			o.isRetryable = f
		}
	}
}

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
)

const (
	// defaultBackoffBaseInMs is the initial duration, in Millisecond
	defaultBackoffBaseInMs = 10.0
	// defaultBackoffCapInMs is the max amount of duration, in Millisecond
	defaultBackoffCapInMs = 100.0
	defaultMaxTries       = 3
)

// Option ...
type Option func(*retryOptions)

// IsRetryable checks the error is safe or worth to retry, eg. "context.Canceled" better not retry
type IsRetryable func(error) bool

type retryOptions struct {
	maxTries        int64
	backoffBaseInMs float64
	backoffCapInMs  float64
	isRetryable     IsRetryable
}

func newRetryOptions() *retryOptions {
	return &retryOptions{
		maxTries:        defaultMaxTries,
		backoffBaseInMs: defaultBackoffBaseInMs,
		backoffCapInMs:  defaultBackoffCapInMs,
		isRetryable:     func(err error) bool { return true },
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

// WithMaxTries configures maximum tries, if tries <= 0 "defaultMaxTries" will be used
func WithMaxTries(tries int64) Option {
	return func(o *retryOptions) {
		if tries > 0 {
			o.maxTries = tries
		}
	}
}

// WithInfiniteTries configures to retry forever (math.MaxInt64 times) till success or got canceled
func WithInfiniteTries() Option {
	return func(o *retryOptions) {
		o.maxTries = math.MaxInt64
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

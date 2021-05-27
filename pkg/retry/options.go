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

// retryOptions ...
type retryOptions struct {
	maxTries    int64
	backoffBase float64
	backoffCap  float64
	// isRetryable checks the error is safe to retry or not, eg. "context.Canceled" better not retry
	isRetryable func(error) bool
}

func newRetryOptions() *retryOptions {
	return &retryOptions{
		maxTries:    defaultMaxTries,
		backoffBase: defaultBackoffBaseInMs,
		backoffCap:  defaultBackoffCapInMs,
		isRetryable: func(err error) bool { return true },
	}
}

// WithBackoffBaseDelay configures the initial delay
func WithBackoffBaseDelay(delayInMs int64) Option {
	return func(o *retryOptions) {
		if delayInMs > 0 {
			o.backoffBase = float64(delayInMs)
		}
	}
}

// WithBackoffMaxDelay configures the maximum delay
func WithBackoffMaxDelay(delayInMs int64) Option {
	return func(o *retryOptions) {
		if delayInMs > 0 {
			o.backoffCap = float64(delayInMs)
		}
	}
}

// WithMaxTries configures maximum tries
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
func WithIsRetryableErr(f func(error) bool) Option {
	return func(o *retryOptions) {
		if f != nil {
			o.isRetryable = f
		}
	}
}

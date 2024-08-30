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

// PreExecution defines a function type that is expected to return an error.
// This function is designed to be executed before retry attempts, but only after the initial execution has failed.
type PreExecution func() error

type retryOptions struct {
	totalRetryDuration time.Duration
	maxTries           uint64
	backoffBaseInMs    float64
	backoffCapInMs     float64
	isRetryable        IsRetryable

	// preExecutionWhenRetry is executed before every retry attempt, except for the initial run.
	// If the first execution fails and a retry is triggered, preExecutionWhenRetry() will be called
	// before the main process is retried. This method ensures that it runs prior to each retry attempt.
	preExecutionWhenRetry PreExecution
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

// WithTotalRetryDuratoin configures the total retry duration.
func WithTotalRetryDuratoin(retryDuration time.Duration) Option {
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

// WithPreExecutionWhenRetry allows you to specify a PreExecution action that will be executed
// before each retry attempt, but only if the initial execution fails. The PreExecution function
// will run before every retry following the first failure, since retries only occur after the
// initial attempt has failed. If the initial execution is successful, the PreExecution function
// will not be triggered, as no retry is necessary.
func WithPreExecutionWhenRetry(f PreExecution) Option {
	return func(o *retryOptions) {
		if f != nil {
			o.preExecutionWhenRetry = f
		}
	}
}

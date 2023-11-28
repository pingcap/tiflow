// Copyright 2023 PingCAP, Inc.
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
	"math/rand"
	"time"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	defaultErrorMaxRetryDuration = 30 * time.Minute
	defaultErrGCInterval         = 10 * time.Minute
	defaultBackoffBaseInS        = 5
	defaultBackoffMaxInS         = 30
)

// ErrorRetry is used to control the error retry logic.
type ErrorRetry struct {
	// To control the error retry.
	lastInternalError  error
	firstRetryTime     time.Time
	lastErrorRetryTime time.Time
	maxRetryDuration   time.Duration
	errGCInterval      time.Duration
	backoffBase        int64
	backoffMax         int64
}

// NewDefaultErrorRetry creates a new ErrorRetry with default values.
func NewDefaultErrorRetry() *ErrorRetry {
	return NewErrorRetry(defaultErrorMaxRetryDuration,
		defaultErrGCInterval,
		defaultBackoffBaseInS,
		defaultBackoffMaxInS)
}

// NewInfiniteErrorRetry creates a new ErrorRetry with infinite duration.
func NewInfiniteErrorRetry() *ErrorRetry {
	return NewErrorRetry(time.Duration(math.MaxInt64),
		defaultErrGCInterval,
		defaultBackoffBaseInS,
		defaultBackoffMaxInS)
}

// NewErrorRetry creates a new ErrorRetry.
func NewErrorRetry(
	maxRetryDuration time.Duration,
	errGCInterval time.Duration,
	backoffBase int64,
	backoffMax int64,
) *ErrorRetry {
	return &ErrorRetry{
		maxRetryDuration: maxRetryDuration,
		errGCInterval:    errGCInterval,
		backoffBase:      backoffBase,
		backoffMax:       backoffMax,
	}
}

// GetRetryBackoff returns the backoff duration for retrying the last error.
// If the retry time is exhausted, it returns the an ChangefeedUnRetryableError.
func (r *ErrorRetry) GetRetryBackoff(err error) (time.Duration, error) {
	// reset firstRetryTime when the last error is too long ago
	// it means the last error is retry success, and the sink is running well for some time
	if r.lastInternalError == nil ||
		time.Since(r.lastErrorRetryTime) >= r.errGCInterval {
		log.Debug("reset firstRetryTime",
			zap.Time("lastErrorRetryTime", r.lastErrorRetryTime),
			zap.Time("now", time.Now()))
		r.firstRetryTime = time.Now()
	}

	// return an unretryable error if retry time is exhausted
	if time.Since(r.firstRetryTime) >= r.maxRetryDuration {
		log.Debug("error retry exhausted",
			zap.Time("firstRetryTime", r.firstRetryTime),
			zap.Time("lastErrorRetryTime", r.lastErrorRetryTime),
			zap.Time("now", time.Now()))
		return 0, cerror.WrapChangefeedUnretryableErr(err)
	}

	r.lastInternalError = err
	r.lastErrorRetryTime = time.Now()

	// interval is in range [defaultBackoffBaseInS, defaultBackoffMaxInS)
	interval := time.Second * time.Duration(
		rand.Int63n(defaultBackoffMaxInS-defaultBackoffBaseInS)+defaultBackoffBaseInS)
	return interval, nil
}

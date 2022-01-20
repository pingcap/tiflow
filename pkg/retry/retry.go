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
	"context"
	"math"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/errors"
)

// Run is Deprecated, try to use Do instead.
// Run retries the specified function on error for at most maxRetries times.
// It stops retry if the returned error is context.Canceled or context.DeadlineExceeded.
func Run(initialInterval time.Duration, maxRetries uint64, f func() error) error {
	cfg := backoff.NewExponentialBackOff()
	cfg.InitialInterval = initialInterval
	retryCfg := backoff.WithMaxRetries(cfg, maxRetries)
	return backoff.Retry(func() error {
		err := f()
		switch errors.Cause(err) {
		case context.Canceled, context.DeadlineExceeded:
			err = backoff.Permanent(err)
		}
		return err
	}, retryCfg)
}

// RunWithInfiniteRetry is Deprecated, try to use instead Do WithInfiniteTries option instead.
// RunWithInfiniteRetry retries the specified function indefinitely, until a backoff.PermanentError is encountered.
// notifyFunc will be called each time before sleeping with the total elapsed time.
func RunWithInfiniteRetry(initialInterval time.Duration, f func() error, notifyFunc func(elapsed time.Duration)) error {
	cfg := backoff.NewExponentialBackOff()
	cfg.InitialInterval = initialInterval
	cfg.MaxElapsedTime = math.MaxInt64

	startTime := time.Now()
	err := backoff.Retry(func() error {
		err := f()
		switch errors.Cause(err) {
		case context.Canceled, context.DeadlineExceeded:
			err = backoff.Permanent(err)
		}

		if _, ok := err.(*backoff.PermanentError); !ok {
			notifyFunc(time.Since(startTime))
		}
		return err
	}, cfg)

	return err
}

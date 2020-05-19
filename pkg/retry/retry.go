// Copyright 2020 PingCAP, Inc.
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
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pingcap/errors"
)

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

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
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// Operation is the action need to retry
type Operation func() error

// Do execute the specified function at most maxTries times until it succeeds or got canceled
func Do(ctx context.Context, operation Operation, opts ...Option) error {
	retryOption := setOptions(opts...)
	return run(ctx, operation, retryOption)
}

func setOptions(opts ...Option) *retryOptions {
	retryOption := newRetryOptions()
	for _, opt := range opts {
		opt(retryOption)
	}
	return retryOption
}

func run(ctx context.Context, op Operation, retryOption *retryOptions) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	var t *time.Timer
	try := 0
	backOff := time.Duration(0)
	for {
		err := op()
		if err == nil {
			return nil
		}

		if !retryOption.isRetryable(err) {
			return err
		}

		try++
		if int64(try) >= retryOption.maxTries {
			return cerror.ErrReachMaxTry.Wrap(err).GenWithStackByArgs(retryOption.maxTries)
		}

		backOff = getBackoffInMs(retryOption.backoffBaseInMs, retryOption.backoffCapInMs, float64(try))
		if t == nil {
			t = time.NewTimer(backOff)
			defer t.Stop()
		} else {
			t.Reset(backOff)
		}

		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-t.C:
		}
	}
}

// getBackoffInMs returns the duration to wait before next try
// See https://www.awsarchitectureblog.com/2015/03/backoff.html
func getBackoffInMs(backoffBaseInMs, backoffCapInMs, try float64) time.Duration {
	temp := int64(math.Min(backoffCapInMs, backoffBaseInMs*math.Exp2(try)) / 2)
	if temp <= 0 {
		temp = 1
	}
	sleep := temp + rand.Int63n(temp)
	backOff := math.Min(backoffCapInMs, float64(rand.Int63n(sleep*3))+backoffBaseInMs)
	return time.Duration(backOff) * time.Millisecond
}

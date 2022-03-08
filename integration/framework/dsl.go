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

package framework

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	waitMaxPollInterval = time.Second * 5
)

// Awaitable represents the handle of an SQL operation that can be waited on
type Awaitable interface {
	SetTimeOut(duration time.Duration) Awaitable
	Wait() Checkable
}

// Checkable represents the handle of an SQL operation whose correctness can be checked
type Checkable interface {
	Check() error
}

type pollable interface {
	poll(ctx context.Context) (bool, error)
}

type pollableAndCheckable interface {
	pollable
	Checkable
}

type errorCheckableAndAwaitable struct {
	error
}

// Check implements Checkable
func (e *errorCheckableAndAwaitable) Check() error {
	return e.error
}

// Wait implements Awaitable
func (e *errorCheckableAndAwaitable) Wait() Checkable {
	return e
}

// SetTimeOut implements Awaitable
func (e *errorCheckableAndAwaitable) SetTimeOut(duration time.Duration) Awaitable {
	return e
}

type basicAwaitable struct {
	pollableAndCheckable
	timeout time.Duration
}

// SetTimeOut implements Awaitable
func (b *basicAwaitable) SetTimeOut(duration time.Duration) Awaitable {
	b.timeout = duration
	return b
}

// Wait implements Awaitable
func (b *basicAwaitable) Wait() Checkable {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if b.timeout == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), b.timeout)
	}
	defer cancel()

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxInterval = waitMaxPollInterval
	for {
		select {
		case <-ctx.Done():
			return &errorCheckableAndAwaitable{ctx.Err()}
		default:
		}

		ok, err := b.poll(ctx)
		if err != nil {
			return &errorCheckableAndAwaitable{errors.Annotate(err, "Wait() failed with error")}
		}

		if ok {
			log.Debug("Wait(): pollable finished")
			return b
		}

		interval := expBackoff.NextBackOff()
		if interval == backoff.Stop {
			return &errorCheckableAndAwaitable{errors.New("Maximum retry interval reached")}
		}
		log.Debug("Wait(): pollable returned false, backing off", zap.Duration("interval", interval))

		ch := time.After(interval)
		select {
		case <-ctx.Done():
			return &errorCheckableAndAwaitable{ctx.Err()}
		case <-ch:
		}
	}
}

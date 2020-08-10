package framework

import (
	"context"
	"time"

	backoff2 "github.com/cenkalti/backoff"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	waitMaxPollInterval = time.Second * 10
)

type Awaitable interface {
	SetTimeOut(duration time.Duration) Awaitable
	Wait() Checkable
}

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

func (e *errorCheckableAndAwaitable) Check() error {
	return e.error
}

func (e *errorCheckableAndAwaitable) Wait() Checkable {
	return e
}

func (e *errorCheckableAndAwaitable) SetTimeOut(duration time.Duration) Awaitable {
	return e
}

type basicAwaitable struct {
	pollableAndCheckable
	timeout time.Duration
}

func (b *basicAwaitable) SetTimeOut(duration time.Duration) Awaitable {
	b.timeout = duration
	return b
}

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

	backoff := backoff2.NewExponentialBackOff()
	backoff.MaxInterval = waitMaxPollInterval

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

		interval := backoff.NextBackOff()
		log.Debug("Wait(): pollable returned false, backing off", zap.Duration("interval", interval))

		ch := time.After(interval)
		select {
		case <-ctx.Done():
			return &errorCheckableAndAwaitable{ctx.Err()}
		case <-ch:
		}
	}
}

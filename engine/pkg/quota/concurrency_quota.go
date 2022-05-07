package quota

import (
	"context"

	"github.com/pingcap/errors"
	"golang.org/x/sync/semaphore"
)

type ConcurrencyQuota interface {
	Consume(ctx context.Context) error
	TryConsume() bool
	Release()
}

func NewConcurrencyQuota(total int64) ConcurrencyQuota {
	return &concurrencyQuotaImpl{sem: semaphore.NewWeighted(total)}
}

type concurrencyQuotaImpl struct {
	sem *semaphore.Weighted
}

func (c *concurrencyQuotaImpl) Consume(ctx context.Context) error {
	return errors.Trace(c.sem.Acquire(ctx, 1))
}

func (c *concurrencyQuotaImpl) TryConsume() bool {
	return c.sem.TryAcquire(1)
}

func (c *concurrencyQuotaImpl) Release() {
	c.sem.Release(1)
}

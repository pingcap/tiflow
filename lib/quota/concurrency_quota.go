package quota

import "golang.org/x/sync/semaphore"

type ConcurrencyQuota interface {
	TryConsume() bool
	Release()
}

func NewConcurrencyQuota(total int64) ConcurrencyQuota {
	return &concurrencyQuotaImpl{sem: semaphore.NewWeighted(total)}
}

type concurrencyQuotaImpl struct {
	sem *semaphore.Weighted
}

func (c *concurrencyQuotaImpl) TryConsume() bool {
	return c.sem.TryAcquire(1)
}

func (c *concurrencyQuotaImpl) Release() {
	c.sem.Release(1)
}

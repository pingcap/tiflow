package autoid

import "sync"

type Allocator struct {
	sync.Mutex
	id int32
}

func NewAllocator() *Allocator {
	return &Allocator{}
}

func (a *Allocator) AllocID() int32 {
	a.Lock()
	defer a.Unlock()
	a.id++
	return a.id
}

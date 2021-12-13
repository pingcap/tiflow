package autoid

import (
	"sync"

	"github.com/google/uuid"
)

type IDAllocator struct {
	sync.Mutex
	id int32
}

func NewAllocator() *IDAllocator {
	return &IDAllocator{}
}

func (a *IDAllocator) AllocID() int32 {
	a.Lock()
	defer a.Unlock()
	a.id++
	return a.id
}

type UUIDAllocator struct{}

func NewUUIDAllocator() *UUIDAllocator {
	return new(UUIDAllocator)
}

func (a *UUIDAllocator) AllocID() string {
	return uuid.New().String()
}

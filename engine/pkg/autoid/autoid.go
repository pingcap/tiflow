package autoid

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

type JobIDAllocator interface {
	AllocJobID() int32
}

type TaskIDAllocator interface {
	AllocTaskID() int64
}

type iDAllocator struct {
	sync.Mutex
	internalID int32
	jobID      int64
}

func NewTaskIDAllocator(jobID int32) (TaskIDAllocator, error) {
	if jobID < 1 {
		return nil, errors.New("job id have to be a positive int")
	}
	return &iDAllocator{
		jobID: int64(jobID) << 32,
	}, nil
}

func NewJobIDAllocator() JobIDAllocator {
	return &iDAllocator{}
}

func (a *iDAllocator) AllocJobID() int32 {
	a.Lock()
	defer a.Unlock()
	a.internalID++
	return a.internalID
}

func (a *iDAllocator) AllocTaskID() int64 {
	a.Lock()
	defer a.Unlock()
	a.internalID++
	return int64(a.internalID) + a.jobID
}

type UUIDAllocator struct{}

func NewUUIDAllocator() *UUIDAllocator {
	return new(UUIDAllocator)
}

func (a *UUIDAllocator) AllocID() string {
	return uuid.New().String()
}

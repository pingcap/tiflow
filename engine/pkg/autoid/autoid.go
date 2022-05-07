// Copyright 2022 PingCAP, Inc.
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

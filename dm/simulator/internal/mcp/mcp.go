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

// Package mcp defines the Modification Candidate Pool (MCP).
package mcp

import (
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

var (
	initRandSeedOnce sync.Once
	gMCPRand         *rand.Rand
	gMCPRandLock     sync.Mutex
)

func init() {
	initRandSeedOnce.Do(func() {
		gMCPRand = rand.New(rand.NewSource(time.Now().Unix()))
	})
}

// ModificationCandidatePool is the core container storing all the current unique keys for a table.
type ModificationCandidatePool struct {
	sync.RWMutex
	keyPool []*UniqueKey
}

// NewModificationCandidatePool create a new MCP.
func NewModificationCandidatePool() *ModificationCandidatePool {
	theKeyPool := make([]*UniqueKey, 0, 8192)
	return &ModificationCandidatePool{
		keyPool: theKeyPool,
	}
}

// NextUK randomly picks a unique key in the MCP.
func (mcp *ModificationCandidatePool) NextUK() *UniqueKey {
	mcp.RLock()
	defer mcp.RUnlock()
	if len(mcp.keyPool) == 0 {
		return nil
	}
	gMCPRandLock.Lock()
	idx := gMCPRand.Intn(len(mcp.keyPool))
	gMCPRandLock.Unlock()
	return mcp.keyPool[idx] // pass by reference
}

// Len gets the current length of the MCP.
func (mcp *ModificationCandidatePool) Len() int {
	mcp.RLock()
	defer mcp.RUnlock()
	return len(mcp.keyPool)
}

// AddUK adds the unique key into the MCP.
// It has side effect: the input UK's row ID will be changed.
func (mcp *ModificationCandidatePool) AddUK(uk *UniqueKey) error {
	mcp.Lock()
	defer mcp.Unlock()
	if len(mcp.keyPool)+1 > cap(mcp.keyPool) {
		return errors.Trace(ErrMCPCapacityFull)
	}
	currentLen := len(mcp.keyPool)
	uk.Lock()
	uk.RowID = currentLen
	uk.Unlock()
	mcp.keyPool = append(mcp.keyPool, uk)
	return nil
}

// DeleteUK deletes the unique key from the MCP.
// It will get the row ID of the UK and delete the UK on that position.
// If the actual value is different from the input UK, the element will still be deleted.
// It has side effect: after the deletion, the input UK's row ID will be set to -1,
// to prevent deleting a dangling UK multiple times.
func (mcp *ModificationCandidatePool) DeleteUK(uk *UniqueKey) error {
	var (
		deletedUK *UniqueKey
		deleteIdx int
	)
	if uk == nil {
		return nil
	}
	mcp.Lock()
	defer mcp.Unlock()
	uk.RLock()
	deleteIdx = uk.RowID
	uk.RUnlock()
	if deleteIdx < 0 {
		return errors.Trace(ErrDeleteUKNotFound)
	}
	if deleteIdx >= len(mcp.keyPool) {
		log.L().Error("the delete UK row ID > MCP's total length", zap.Int("delete row ID", deleteIdx), zap.Int("current key pool length", len(mcp.keyPool)))
		return errors.Trace(ErrInvalidRowID)
	}
	deletedUK = mcp.keyPool[deleteIdx]
	curLen := len(mcp.keyPool)
	lastUK := mcp.keyPool[curLen-1]
	lastUK.Lock()
	lastUK.RowID = deleteIdx
	lastUK.Unlock()
	mcp.keyPool[deleteIdx] = lastUK
	mcp.keyPool[curLen-1] = deletedUK
	mcp.keyPool = mcp.keyPool[:curLen-1]
	deletedUK.Lock()
	deletedUK.RowID = -1
	deletedUK.Unlock()
	return nil
}

// Reset cleans up all the items in the MCP.
func (mcp *ModificationCandidatePool) Reset() {
	mcp.keyPool = mcp.keyPool[:0]
}

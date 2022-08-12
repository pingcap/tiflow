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

package syncutil

import (
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const warnRCUBlockedThreshold = 10 * time.Second

// RCU is used for implement pseudo-RCU semantics.
type RCU struct {
	ch chan struct{}
}

// NewRCU creates a new RCU object.
func NewRCU() *RCU {
	return &RCU{ch: make(chan struct{})}
}

// Synchronize waits for Quiesce to be called at least once.
func (r *RCU) Synchronize() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()
	for {
		select {
		case r.ch <- struct{}{}:
			return
		case <-ticker.C:
			duration := time.Since(startTime)
			if duration > warnRCUBlockedThreshold {
				log.Warn("synchronize taking too long",
					zap.Duration("duration", duration),
					zap.StackSkip("stack", 1))
			}
		}
	}
}

// Quiesce makes pending Synchronize calls return.
func (r *RCU) Quiesce() {
	select {
	case <-r.ch:
	default:
	}
}

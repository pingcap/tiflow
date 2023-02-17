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

package fakejob

import (
	"encoding/json"
	"sync"
	"time"
)

// DummyWorkerStatus is used in fake worker as status.
type DummyWorkerStatus struct {
	rwm        sync.RWMutex
	BusinessID int               `json:"business-id"`
	Tick       int64             `json:"tick"`
	Checkpoint *WorkerCheckpoint `json:"checkpoint"`
}

// DoTick is a periodically tick function.
func (s *DummyWorkerStatus) DoTick() {
	s.rwm.Lock()
	defer s.rwm.Unlock()
	s.Tick++
}

// GetEtcdCheckpoint returns cached checkpoint stored in etcd.
func (s *DummyWorkerStatus) GetEtcdCheckpoint() WorkerCheckpoint {
	s.rwm.RLock()
	defer s.rwm.RUnlock()
	return *s.Checkpoint
}

// SetEtcdCheckpoint sets checkpoint to memory cache.
func (s *DummyWorkerStatus) SetEtcdCheckpoint(ckpt *WorkerCheckpoint) {
	s.rwm.Lock()
	defer s.rwm.Unlock()
	s.Checkpoint = ckpt
}

// Marshal returns the JSON encoding of DummyWorkerStatus.
func (s *DummyWorkerStatus) Marshal() ([]byte, error) {
	s.rwm.RLock()
	defer s.rwm.RUnlock()
	return json.Marshal(s)
}

// Unmarshal parses the JSON-encoded data and stores the result in DummyWorkerStatus.
func (s *DummyWorkerStatus) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

// WorkerCheckpoint is used to resume a new worker from old checkpoint
type WorkerCheckpoint struct {
	Tick      int64  `json:"tick"`
	Revision  int64  `json:"revision"`
	MvccCount int    `json:"mvcc-count"`
	Value     string `json:"value"`
}

// WorkerConfig defines config of fake worker
type WorkerConfig struct {
	ID         int   `json:"id"`
	TargetTick int64 `json:"target-tick"`

	EtcdWatchEnable     bool          `json:"etcd-watch-enable"`
	EtcdEndpoints       []string      `json:"etcd-endpoints"`
	EtcdWatchPrefix     string        `json:"etcd-watch-prefix"`
	InjectErrorInterval time.Duration `json:"inject-error-interval"`

	Checkpoint WorkerCheckpoint `json:"checkpoint"`
}

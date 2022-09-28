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

package jobop

import "github.com/pingcap/tiflow/engine/pkg/clock"

// BackoffManager manages backoff of a target job set
type BackoffManager interface {
	Terminate(jobID string) bool
	Allow(jobID string) bool
	JobOnline(jobID string)
	JobFail(jobID string)
	JobTerminate(jobID string)
}

// BackoffManagerImpl manages JobBackoff of all running or retrying jobs, it
// implement interface BackoffManager.
type BackoffManagerImpl struct {
	jobs    map[string]*JobBackoff
	clocker clock.Clock
	config  *BackoffConfig
}

// NewBackoffManagerImpl creates a new backoff manager
func NewBackoffManagerImpl(clocker clock.Clock, config *BackoffConfig) *BackoffManagerImpl {
	return &BackoffManagerImpl{
		jobs:    make(map[string]*JobBackoff),
		clocker: clocker,
		config:  config,
	}
}

// Terminate checks whether this job should be terminated, terminated means
// job manager won't create this job any more.
func (m *BackoffManagerImpl) Terminate(jobID string) bool {
	backoff, ok := m.jobs[jobID]
	if !ok {
		return false
	}
	return backoff.Terminate()
}

// Allow checks whether this job can be created now
func (m *BackoffManagerImpl) Allow(jobID string) bool {
	backoff, ok := m.jobs[jobID]
	if !ok {
		return true
	}
	return backoff.Allow()
}

// JobOnline means a job is online
func (m *BackoffManagerImpl) JobOnline(jobID string) {
	m.ensureJobBackoffExists(jobID)
	m.jobs[jobID].Success()
}

// JobFail means a job is offline(with error) or dispatched with error
func (m *BackoffManagerImpl) JobFail(jobID string) {
	m.ensureJobBackoffExists(jobID)
	m.jobs[jobID].Fail()
}

// JobTerminate means a job is finished, canceled or failed
func (m *BackoffManagerImpl) JobTerminate(jobID string) {
	delete(m.jobs, jobID)
}

func (m *BackoffManagerImpl) ensureJobBackoffExists(jobID string) {
	if _, ok := m.jobs[jobID]; !ok {
		m.jobs[jobID] = NewJobBackoff(jobID, m.clocker, m.config)
	}
}

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

// BackoffManager manages JobBackoff of all running or retrying jobs
type BackoffManager struct {
	jobs map[string]*JobBackoff
	opts []BackoffOption
}

// NewBackoffManager creates a new backoff manager
func NewBackoffManager(opts ...BackoffOption) *BackoffManager {
	return &BackoffManager{
		jobs: make(map[string]*JobBackoff),
		opts: opts,
	}
}

// Allow checks whether this job can be created now
func (m *BackoffManager) Allow(jobID string) bool {
	backoff, ok := m.jobs[jobID]
	if !ok {
		return true
	}
	return backoff.Allow()
}

// JobOnline means a job is online
func (m *BackoffManager) JobOnline(jobID string) {
	if _, ok := m.jobs[jobID]; !ok {
		m.jobs[jobID] = NewJobBackoff(jobID, m.opts...)
	}
	m.jobs[jobID].Success()
}

// JobFail means a job is offline(with error) or dispatched with error
func (m *BackoffManager) JobFail(jobID string) {
	if _, ok := m.jobs[jobID]; !ok {
		m.jobs[jobID] = NewJobBackoff(jobID, m.opts...)
	}
	m.jobs[jobID].Fail()
}

// JobTerminate means a job is finished, canceled or failed
func (m *BackoffManager) JobTerminate(jobID string) {
	delete(m.jobs, jobID)
}

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

package config

import "time"

// TimeoutConfig defines timeout related config used in master-worker framework
type TimeoutConfig struct {
	WorkerTimeoutDuration            time.Duration
	WorkerTimeoutGracefulDuration    time.Duration
	WorkerHeartbeatInterval          time.Duration
	WorkerReportStatusInterval       time.Duration
	MasterHeartbeatCheckLoopInterval time.Duration
	CloseWorkerTimeout               time.Duration
}

var defaultTimeoutConfig = TimeoutConfig{
	WorkerTimeoutDuration:            time.Second * 15,
	WorkerTimeoutGracefulDuration:    time.Second * 5,
	WorkerHeartbeatInterval:          time.Second * 3,
	WorkerReportStatusInterval:       time.Second * 3,
	MasterHeartbeatCheckLoopInterval: time.Second * 1,
	CloseWorkerTimeout:               time.Second * 3,
}.Adjust()

// Adjust validates the TimeoutConfig and adjusts it
func (config TimeoutConfig) Adjust() TimeoutConfig {
	var tc TimeoutConfig = config
	// worker timeout duration must be 2 times larger than worker heartbeat interval
	if tc.WorkerTimeoutDuration < 2*tc.WorkerHeartbeatInterval+time.Second*3 {
		tc.WorkerTimeoutDuration = 2*tc.WorkerHeartbeatInterval + time.Second*3
	}
	return tc
}

// DefaultTimeoutConfig returns a default timeout config
func DefaultTimeoutConfig() TimeoutConfig {
	return defaultTimeoutConfig
}

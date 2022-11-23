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

package model

// DeployNodeID means the identify of a node
type DeployNodeID string

// ExecutorID is an alias for executor when NodeType is NodeTypeExecutor.
type ExecutorID = DeployNodeID

// ExecutorStatus describes the node aliveness status of an executor
type ExecutorStatus int32

// All ExecutorStatus
const (
	Initing ExecutorStatus = iota
	Running
	Disconnected
	Tombstone
)

// ExecutorStatusNameMapping maps from executor status to human-readable string
var ExecutorStatusNameMapping = map[ExecutorStatus]string{
	Initing:      "initializing",
	Running:      "running",
	Disconnected: "disconnected",
	Tombstone:    "tombstone",
}

// String implements fmt.Stringer
func (s ExecutorStatus) String() string {
	val, ok := ExecutorStatusNameMapping[s]
	if !ok {
		return "unknown"
	}
	return val
}

// ExecutorStatusChangeType describes the types of ExecutorStatusChange.
type ExecutorStatusChangeType string

const (
	// EventExecutorOnline indicates that an executor has gone online.
	EventExecutorOnline = ExecutorStatusChangeType("online")
	// EventExecutorOffline indicates that an executor has gone offline.
	EventExecutorOffline = ExecutorStatusChangeType("offline")
)

// ExecutorStatusChange describes an event where an
// executor's status has changed.
type ExecutorStatusChange struct {
	ID   ExecutorID
	Tp   ExecutorStatusChangeType
	Addr string
}

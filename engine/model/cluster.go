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

import (
	"encoding/json"

	"github.com/pingcap/tiflow/engine/pkg/adapter"
)

// NodeType is the type of a server instance, could be either server master or executor
type NodeType int

// All node types
const (
	NodeTypeServerMaster NodeType = iota + 1
	NodeTypeExecutor
)

// RescUnit is the min unit of resource that we count.
type RescUnit int

// DeployNodeID means the identify of a node
type DeployNodeID string

// ExecutorID is an alias for executor when NodeType is NodeTypeExecutor.
type ExecutorID = DeployNodeID

// NodeInfo describes the information of server instance, contains node type, node
// uuid, advertise address and capability(executor node only)
type NodeInfo struct {
	Type NodeType     `json:"type"`
	ID   DeployNodeID `json:"id"`
	Addr string       `json:"addr"`

	// The capability of executor, including
	// 1. cpu (goroutines)
	// 2. memory
	// 3. disk cap
	// TODO: So we should enrich the cap dimensions in the future.
	Capability int `json:"cap"`
}

// EtcdKey return encoded key for a node used in service discovery etcd
func (e *NodeInfo) EtcdKey() string {
	return adapter.NodeInfoKeyAdapter.Encode(string(e.ID))
}

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

// ToJSON returns json marshal of a node info
func (e *NodeInfo) ToJSON() (string, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return string(data), nil
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
	ID ExecutorID
	Tp ExecutorStatusChangeType
}

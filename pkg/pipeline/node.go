// Copyright 2020 PingCAP, Inc.
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

package pipeline

// Node represents a handle unit for the message stream in pipeline
type Node interface {
	// Receive receives the message from the previous node
	Receive(c Context) error
}

// NodeFunc represents a handle unit for the message stream in pipeline
type NodeFunc func(ctx Context) error

// Receive receives the message from the previous node
func (f NodeFunc) Receive(ctx Context) error {
	return f(ctx)
}

type concurrentNodeGroup struct {
	nodeGroup    []Node
	currentIndex int
}

// NewConcurrentNodeGroup returns a concurrent node group
func NewConcurrentNodeGroup(nodes ...Node) Node {
	return &concurrentNodeGroup{
		nodeGroup: nodes,
	}
}

func (cng *concurrentNodeGroup) Receive(ctx Context) error {
	err := cng.nodeGroup[cng.currentIndex].Receive(ctx)
	cng.currentIndex = (cng.currentIndex + 1) % len(cng.nodeGroup)
	return err
}

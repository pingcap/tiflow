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

package util

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/model"
)

const (
	// the maximum depth of the DAG
	// TODO add a user configurable parameter
	defaultMaximalDepth = 100
)

// DAGWalker walks the DAG and calls the callback function for each node.
// NOTE: We use a struct instead of a function to provide better extensibility
// for the future in case we want to implement more complicated graph algorithms.
type DAGWalker struct {
	visited      map[model.NodeID]struct{}
	onVertex     func(*model.Node) error
	maximalDepth int
}

// NewDAGWalker creates a new DAGWalker.
func NewDAGWalker(onVertex func(*model.Node) error) *DAGWalker {
	return &DAGWalker{
		onVertex:     onVertex,
		maximalDepth: defaultMaximalDepth,
	}
}

// Walk walks the DAG and calls the callback function for each node.
func (w *DAGWalker) Walk(dag *model.DAG) error {
	w.visited = make(map[model.NodeID]struct{})
	return w.doWalk(dag.Root, 0)
}

func (w *DAGWalker) doWalk(node *model.Node, depth int) error {
	if node == nil {
		log.Panic("unexpected nil node")
		return nil // to make the linter happy
	}

	if _, ok := w.visited[node.ID]; ok {
		return nil
	}

	if depth > w.maximalDepth {
		return cerrors.ErrPlannerDAGDepthExceeded.GenWithStackByArgs(depth)
	}

	if err := w.onVertex(node); err != nil {
		return errors.Trace(err)
	}
	w.visited[node.ID] = struct{}{}
	for _, nextNode := range node.Outputs {
		if err := w.doWalk(nextNode, depth+1); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/stretchr/testify/require"
)

func getSimpleDAG() *model.DAG {
	node3 := &model.Node{
		ID: 3,
	}

	return &model.DAG{
		Root: &model.Node{
			ID: 0,
			Outputs: []*model.Node{
				{
					ID: 1,
					Outputs: []*model.Node{
						node3,
					},
				},
				{
					ID: 2,
					Outputs: []*model.Node{
						node3,
					},
				},
			},
		},
	}
}

func TestDAGWalkerWalk(t *testing.T) {
	dag := getSimpleDAG()

	var visited []model.NodeID
	walker := NewDAGWalker(func(node *model.Node) error {
		visited = append(visited, node.ID)
		return nil
	})

	err := walker.Walk(dag)
	require.NoError(t, err)

	require.ElementsMatch(t, []model.NodeID{0, 1, 2, 3}, visited)
}

func TestDAGWalkerWalkError(t *testing.T) {
	dag := getSimpleDAG()

	walker := NewDAGWalker(func(node *model.Node) error {
		if node.ID == 3 {
			return errors.New("dag walk error")
		}
		return nil
	})

	err := walker.Walk(dag)
	require.Error(t, err)
	require.Regexp(t, "dag walk error", err.Error())
}

func TestDAGWalkerWalkTooDeep(t *testing.T) {
	dag := getSimpleDAG()

	walker := NewDAGWalker(func(node *model.Node) error {
		return nil
	})
	walker.maximalDepth = 1

	err := walker.Walk(dag)
	require.Error(t, err)
	require.Regexp(t, ".*ErrPlannerDAGDepthExceeded.*", err.Error())
}

func TestNewDAGWalkerWalkNilNode(t *testing.T) {
	walker := NewDAGWalker(func(node *model.Node) error {
		return nil
	})

	require.Panicsf(t, func() {
		_ = walker.Walk(nil)
	}, "Walk should panic when node is nil")
}

package util

import (
	"testing"

	"github.com/hanfei1991/microcosm/model"
	"github.com/pingcap/errors"
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

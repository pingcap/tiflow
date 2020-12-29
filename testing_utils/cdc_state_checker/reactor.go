package main

import (
	"context"

	"github.com/pingcap/ticdc/pkg/orchestrator"
)

type CDCMonitReactor struct {
	state *CDCReactorState
}

func (r *CDCMonitReactor) Tick(ctx context.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	r.state = state.(*CDCReactorState)
	return r.state, nil
}


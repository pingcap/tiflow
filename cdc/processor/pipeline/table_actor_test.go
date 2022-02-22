// Copyright 2021 PingCAP, Inc.
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

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/stretchr/testify/require"
)

func TestAsyncStopFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	tableActorSystem, tableActorRouter := actor.NewSystemBuilder("table").Build()
	tableActorSystem.Start(ctx)
	defer func() {
		cancel()
		_ = tableActorSystem.Stop()
	}()

	tbl := &tableActor{
		stopped:   0,
		tableID:   1,
		router:    tableActorRouter,
		cancel:    func() {},
		reportErr: func(err error) {},
	}
	require.True(t, tbl.AsyncStop(1))

	mb := actor.NewMailbox(actor.ID(1), 0)
	tbl.actorID = actor.ID(1)
	require.Nil(t, tableActorSystem.Spawn(mb, tbl))
	tbl.mb = mb
	require.Nil(t, tableActorSystem.Stop())
	require.False(t, tbl.AsyncStop(1))
}

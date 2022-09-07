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

package system

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/tiflow/pkg/actor"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
)

// System manages table pipeline global resource.
type System struct {
	tableActorSystem *actor.System[pmessage.Message]
	tableActorRouter *actor.Router[pmessage.Message]

	lastID uint64
}

// NewSystem returns a system.
func NewSystem() *System {
	return &System{
		lastID: 1,
	}
}

// Start starts a system.
func (s *System) Start(ctx context.Context) {
	// todo: make the table actor system configurable
	sys, router := actor.NewSystemBuilder[pmessage.Message]("table").Build()
	s.tableActorSystem, s.tableActorRouter = sys, router
	s.tableActorSystem.Start(ctx)
}

// Stop stops a system.
func (s *System) Stop() {
	s.tableActorSystem.Stop()
}

// Router returns the table actor router.
func (s *System) Router() *actor.Router[pmessage.Message] {
	return s.tableActorRouter
}

// System returns the system.
func (s *System) System() *actor.System[pmessage.Message] {
	return s.tableActorSystem
}

// ActorID returns an ActorID correspond with tableID.
func (s *System) ActorID() actor.ID {
	return actor.ID(atomic.AddUint64(&s.lastID, 1))
}

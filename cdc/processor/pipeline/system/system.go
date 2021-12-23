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
	"fmt"
	"sync"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/actor"
)

// System manages table pipeline global resource.
type System struct {
	tableActorSystem *actor.System
	tableActorRouter *actor.Router

	// actorIDMap store all allocated ID for changefeed-table  -> ID pair
	actorIDMap          map[string]uint64
	actorIDGeneratorLck sync.Mutex
	lastID              uint64
}

// NewSystem returns a system.
func NewSystem() *System {
	return &System{
		actorIDMap: map[string]uint64{},
		lastID:     1,
	}
}

// Start starts a system.
func (s *System) Start(ctx context.Context) error {
	// todo: make the table actor system configurable
	s.tableActorSystem, s.tableActorRouter = actor.NewSystemBuilder("table").Build()
	s.tableActorSystem.Start(ctx)
	return nil
}

// Stop stops a system.
func (s *System) Stop() error {
	return s.tableActorSystem.Stop()
}

func (s *System) Router() *actor.Router {
	return s.tableActorRouter
}

func (s *System) System() *actor.System {
	return s.tableActorSystem
}

// ActorID returns an ActorID correspond with tableID.
func (s *System) ActorID(changefeedID string, tableID model.TableID) actor.ID {
	s.actorIDGeneratorLck.Lock()
	defer s.actorIDGeneratorLck.Unlock()

	key := fmt.Sprintf("%s-%d", changefeedID, tableID)
	id, ok := s.actorIDMap[key]
	if !ok {
		s.lastID++
		id = s.lastID
		s.actorIDMap[key] = id
	}
	return actor.ID(id)
}

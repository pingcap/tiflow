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

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/actor"
)

// System manages table pipeline global resource.
type System struct {
	tableActorSystem *actor.System
	tableActorRouter *actor.Router
	idLock           sync.Mutex
	idMap            map[string]uint64
	lastID           uint64
}

// NewSystem returns a system.
func NewSystem() *System {
	return &System{
		idMap:  map[string]uint64{},
		lastID: 1,
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
	s.idLock.Lock()
	defer s.idLock.Unlock()

	key := fmt.Sprintf("%s-%d", changefeedID, tableID)
	id, ok := s.idMap[key]
	if !ok {
		s.lastID++
		id = s.lastID
		s.idMap[key] = id
	}
	return actor.ID(id)
}

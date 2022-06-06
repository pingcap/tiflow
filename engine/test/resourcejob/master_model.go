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

package resourcejob

import (
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
)

type masterState string

const (
	masterStateUninit     = masterState("uninit")
	masterStateInProgress = masterState("in-progress")
	masterStateFinished   = masterState("finished")
)

type resourceState string

const (
	resourceStateUninit     = resourceState("uninit")
	resourceStateUnsorted   = resourceState("unsorted")
	resourceStateCommitting = resourceState("committing")
	resourceStateSorted     = resourceState("sorted")
)

type masterStatus struct {
	State     masterState `json:"state"`
	Resources map[resModel.ResourceID]resourceState
	Bindings  map[libModel.WorkerID]resModel.ResourceID
}

func initialMasterStatus() *masterStatus {
	return &masterStatus{
		State:     masterStateUninit,
		Resources: make(map[resModel.ResourceID]resourceState),
		Bindings:  make(map[libModel.WorkerID]resModel.ResourceID),
	}
}

func (s *masterStatus) OnFinishedCreatingResources() {
	if s.State != masterStateUninit {
		log.L().Panic("OnFinishedCreatingResources: unexpected master state",
			zap.String("state", string(s.State)))
	}

	s.State = masterStateInProgress
}

func (s *masterStatus) OnResourceCreated(resourceID resModel.ResourceID) {
	log.L().Info("OnResourceCreated",
		zap.String("resource-id", resourceID))

	if s.State != masterStateUninit {
		log.L().Panic("unexpected master state", zap.String("state", string(s.State)))
	}

	_, exists := s.Resources[resourceID]
	if exists {
		log.L().Panic("duplicate resource", zap.String("resource-id", resourceID))
	}

	s.Resources[resourceID] = resourceStateUninit
}

func (s *masterStatus) OnResourceInitialized(resourceID resModel.ResourceID, workerID libModel.WorkerID) {
	log.L().Info("OnResourceInitialized",
		zap.String("resource-id", resourceID))

	resourceID, ok := s.Bindings[workerID]
	if !ok {
		log.L().Panic("worker not found", zap.String("worker-id", workerID))
	}

	oldState, exists := s.Resources[resourceID]
	if !exists {
		log.L().Panic("resource state not found",
			zap.String("worker-id", workerID),
			zap.String("resource-id", resourceID))
	}
	if oldState != resourceStateUninit {
		log.L().Panic("resource state unexpected",
			zap.String("worker-id", workerID),
			zap.String("resource-id", resourceID),
			zap.String("unexpected state", string(oldState)))
	}

	s.Resources[resourceID] = resourceStateUnsorted
}

func (s *masterStatus) OnBindResourceToWorker(resourceID resModel.ResourceID, workerID libModel.WorkerID) {
	log.L().Info("OnBindResourceToWorker",
		zap.String("resource-id", resourceID),
		zap.String("worker-id", workerID))

	if s.State != masterStateInProgress {
		log.L().Panic("unexpected master state", zap.String("state", string(s.State)))
	}

	// Check for duplicates
	for _, resID := range s.Bindings {
		if resourceID == resID {
			log.L().Panic("resource is already bound",
				zap.String("resource-id", resourceID),
				zap.String("worker-id", workerID))
		}
	}

	resID, exists := s.Bindings[workerID]
	if exists {
		log.L().Panic("worker is already bound to",
			zap.String("worker-id", workerID),
			zap.String("already-bound-resource-id", resID))
	}

	// Everything is okay
	s.Bindings[resourceID] = workerID
}

func (s *masterStatus) OnUnbindWorker(workerID libModel.WorkerID) {
	log.L().Info("OnUnbindWorker",
		zap.String("worker-id", workerID))

	delete(s.Bindings, workerID)
}

func (s *masterStatus) OnWorkerStartedCopying(workerID libModel.WorkerID) {
	log.L().Info("OnWorkerStartedCopying",
		zap.String("worker-id", workerID))

	resourceID, ok := s.Bindings[workerID]
	if !ok {
		log.L().Panic("worker not found", zap.String("worker-id", workerID))
	}

	oldState, exists := s.Resources[resourceID]
	if !exists {
		log.L().Panic("resource state not found",
			zap.String("worker-id", workerID),
			zap.String("resource-id", resourceID))
	}
	if oldState != resourceStateUnsorted {
		log.L().Panic("resource state unexpected",
			zap.String("worker-id", workerID),
			zap.String("resource-id", resourceID),
			zap.String("unexpected state", string(oldState)))
	}

	s.Resources[resourceID] = resourceStateCommitting
}

func (s *masterStatus) GetResourceStateForWorker(workerID libModel.WorkerID) (resourceState, bool) {
	resID, ok := s.Bindings[workerID]
	if !ok {
		return "", false
	}

	resState, ok := s.Resources[resID]
	if !ok {
		log.L().Panic("GetResourceStateForWorker: resource not found",
			zap.String("worker-id", workerID),
			zap.String("resource-id", resID))
	}

	return resState, true
}

func (s *masterStatus) OnWorkerFinishedCopying(workerID libModel.WorkerID) {
	log.L().Info("OnWorkerFinishedCopying",
		zap.String("worker-id", workerID))

	resourceID, ok := s.Bindings[workerID]
	if !ok {
		log.L().Panic("worker not found", zap.String("worker-id", workerID))
	}

	oldState, exists := s.Resources[resourceID]
	if !exists {
		log.L().Panic("resource state not found",
			zap.String("worker-id", workerID),
			zap.String("resource-id", resourceID))
	}
	if oldState != resourceStateCommitting {
		log.L().Panic("resource state unexpected",
			zap.String("worker-id", workerID),
			zap.String("resource-id", resourceID),
			zap.String("unexpected state", string(oldState)))
	}

	s.Resources[resourceID] = resourceStateSorted
}

func (s *masterStatus) CountResourceByState(state resourceState) int {
	ret := 0
	for _, st := range s.Resources {
		if st == state {
			ret++
		}
	}
	return ret
}

func (s *masterStatus) OnAllResourcesSorted() {
	if s.State != masterStateInProgress {
		log.L().Panic("OnFinishedCreatingResources: unexpected master state",
			zap.String("state", string(s.State)))
	}

	s.State = masterStateFinished
}

func (s *masterStatus) UnboundResources() map[resModel.ResourceID]struct{} {
	unboundResourceSet := make(map[resModel.ResourceID]struct{})
	// Add all resources to the set.
	for resID := range s.Resources {
		unboundResourceSet[resID] = struct{}{}
	}
	// Remove bound resources.
	for _, resID := range s.Bindings {
		delete(unboundResourceSet, resID)
	}
	return unboundResourceSet
}

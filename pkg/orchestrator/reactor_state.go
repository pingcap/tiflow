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

package orchestrator

import (
	"encoding/json"
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator/util"
	"go.uber.org/zap"
)

// GlobalReactorState represents a global state which stores all key-value pairs in ETCD
type GlobalReactorState struct {
	Owner          map[string]struct{}
	Captures       map[model.CaptureID]*model.CaptureInfo
	Changefeeds    map[model.ChangeFeedID]*ChangefeedReactorState
	pendingPatches [][]DataPatch

	// onCaptureAdded and onCaptureRemoved are hook functions
	// to be called when captures are added and removed.
	onCaptureAdded   func(captureID model.CaptureID, addr string)
	onCaptureRemoved func(captureID model.CaptureID)
}

// NewGlobalState creates a new global state
func NewGlobalState() *GlobalReactorState {
	return &GlobalReactorState{
		Owner:       map[string]struct{}{},
		Captures:    make(map[model.CaptureID]*model.CaptureInfo),
		Changefeeds: make(map[model.ChangeFeedID]*ChangefeedReactorState),
	}
}

// Update implements the ReactorState interface
func (s *GlobalReactorState) Update(key util.EtcdKey, value []byte, _ bool) error {
	k := new(etcd.CDCKey)
	err := k.Parse(key.String())
	if err != nil {
		return errors.Trace(err)
	}
	switch k.Tp {
	case etcd.CDCKeyTypeOwner:
		if value != nil {
			s.Owner[k.OwnerLeaseID] = struct{}{}
		} else {
			delete(s.Owner, k.OwnerLeaseID)
		}
		return nil
	case etcd.CDCKeyTypeCapture:
		if value == nil {
			log.Info("remote capture offline",
				zap.String("captureID", k.CaptureID),
				zap.Any("info", s.Captures[k.CaptureID]))
			delete(s.Captures, k.CaptureID)
			if s.onCaptureRemoved != nil {
				s.onCaptureRemoved(k.CaptureID)
			}
			return nil
		}

		var newCaptureInfo model.CaptureInfo
		err := newCaptureInfo.Unmarshal(value)
		if err != nil {
			return cerrors.ErrUnmarshalFailed.Wrap(err).GenWithStackByArgs()
		}

		log.Info("remote capture online",
			zap.String("captureID", k.CaptureID), zap.Any("info", newCaptureInfo))
		if s.onCaptureAdded != nil {
			s.onCaptureAdded(k.CaptureID, newCaptureInfo.AdvertiseAddr)
		}
		s.Captures[k.CaptureID] = &newCaptureInfo
	case etcd.CDCKeyTypeChangefeedInfo,
		etcd.CDCKeyTypeChangeFeedStatus,
		etcd.CDCKeyTypeTaskPosition,
		etcd.CDCKeyTypeTaskStatus,
		etcd.CDCKeyTypeTaskWorkload:
		changefeedState, exist := s.Changefeeds[k.ChangefeedID]
		if !exist {
			if value == nil {
				return nil
			}
			changefeedState = NewChangefeedReactorState(k.ChangefeedID)
			s.Changefeeds[k.ChangefeedID] = changefeedState
		}
		if err := changefeedState.UpdateCDCKey(k, value); err != nil {
			return errors.Trace(err)
		}
		if value == nil && !changefeedState.Exist() {
			s.pendingPatches = append(s.pendingPatches, changefeedState.getPatches())
			delete(s.Changefeeds, k.ChangefeedID)
		}
	default:
		log.Warn("receive an unexpected etcd event", zap.String("key", key.String()), zap.ByteString("value", value))
	}
	return nil
}

// GetPatches implements the ReactorState interface
// Every []DataPatch slice in [][]DataPatch slice is the patches of a ChangefeedReactorState
func (s *GlobalReactorState) GetPatches() [][]DataPatch {
	pendingPatches := s.pendingPatches
	for _, changefeedState := range s.Changefeeds {
		pendingPatches = append(pendingPatches, changefeedState.getPatches())
	}
	s.pendingPatches = nil
	return pendingPatches
}

// SetOnCaptureAdded registers a function that is called when a capture goes online.
func (s *GlobalReactorState) SetOnCaptureAdded(f func(captureID model.CaptureID, addr string)) {
	s.onCaptureAdded = f
}

// SetOnCaptureRemoved registers a function that is called when a capture goes offline.
func (s *GlobalReactorState) SetOnCaptureRemoved(f func(captureID model.CaptureID)) {
	s.onCaptureRemoved = f
}

// ChangefeedReactorState represents a changefeed state which stores all key-value pairs of a changefeed in ETCD
type ChangefeedReactorState struct {
	ID            model.ChangeFeedID
	Info          *model.ChangeFeedInfo
	Status        *model.ChangeFeedStatus
	TaskPositions map[model.CaptureID]*model.TaskPosition

	// Deprecated: No longer used, kept for compatibility.
	TaskStatuses map[model.CaptureID]*model.TaskStatus
	// Deprecated: No longer used, kept for compatibility.
	Workloads map[model.CaptureID]model.TaskWorkload

	pendingPatches        []DataPatch
	skipPatchesInThisTick bool
}

// NewChangefeedReactorState creates a new changefeed reactor state
func NewChangefeedReactorState(id model.ChangeFeedID) *ChangefeedReactorState {
	return &ChangefeedReactorState{
		ID:            id,
		TaskPositions: make(map[model.CaptureID]*model.TaskPosition),
		TaskStatuses:  make(map[model.CaptureID]*model.TaskStatus),
		Workloads:     make(map[model.CaptureID]model.TaskWorkload),
	}
}

// Update implements the ReactorState interface
func (s *ChangefeedReactorState) Update(key util.EtcdKey, value []byte, _ bool) error {
	k := new(etcd.CDCKey)
	if err := k.Parse(key.String()); err != nil {
		return errors.Trace(err)
	}
	if err := s.UpdateCDCKey(k, value); err != nil {
		log.Error("failed to update status", zap.String("key", key.String()), zap.ByteString("value", value))
		return errors.Trace(err)
	}
	return nil
}

// UpdateCDCKey updates the state by a parsed etcd key
func (s *ChangefeedReactorState) UpdateCDCKey(key *etcd.CDCKey, value []byte) error {
	var e interface{}
	switch key.Tp {
	case etcd.CDCKeyTypeChangefeedInfo:
		if key.ChangefeedID != s.ID {
			return nil
		}
		if value == nil {
			s.Info = nil
			return nil
		}
		s.Info = new(model.ChangeFeedInfo)
		e = s.Info
	case etcd.CDCKeyTypeChangeFeedStatus:
		if key.ChangefeedID != s.ID {
			return nil
		}
		if value == nil {
			s.Status = nil
			return nil
		}
		s.Status = new(model.ChangeFeedStatus)
		e = s.Status
	case etcd.CDCKeyTypeTaskPosition:
		if key.ChangefeedID != s.ID {
			return nil
		}
		if value == nil {
			delete(s.TaskPositions, key.CaptureID)
			return nil
		}
		position := new(model.TaskPosition)
		s.TaskPositions[key.CaptureID] = position
		e = position
	case etcd.CDCKeyTypeTaskStatus:
		if key.ChangefeedID != s.ID {
			return nil
		}
		if value == nil {
			delete(s.TaskStatuses, key.CaptureID)
			return nil
		}
		status := new(model.TaskStatus)
		s.TaskStatuses[key.CaptureID] = status
		e = status
	case etcd.CDCKeyTypeTaskWorkload:
		if key.ChangefeedID != s.ID {
			return nil
		}
		if value == nil {
			delete(s.Workloads, key.CaptureID)
			return nil
		}
		workload := make(model.TaskWorkload)
		s.Workloads[key.CaptureID] = workload
		e = &workload
	default:
		return nil
	}
	if err := json.Unmarshal(value, e); err != nil {
		return errors.Trace(err)
	}
	if key.Tp == etcd.CDCKeyTypeChangefeedInfo {
		if err := s.Info.VerifyAndComplete(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Exist returns false if all keys of this changefeed in ETCD is not exist
func (s *ChangefeedReactorState) Exist() bool {
	return s.Info != nil || s.Status != nil || len(s.TaskPositions) != 0 || len(s.TaskStatuses) != 0 || len(s.Workloads) != 0
}

// Active return true if the changefeed is ready to be processed
func (s *ChangefeedReactorState) Active(captureID model.CaptureID) bool {
	return s.Info != nil && s.Status != nil && s.Status.AdminJobType == model.AdminNone
}

// GetPatches implements the ReactorState interface
func (s *ChangefeedReactorState) GetPatches() [][]DataPatch {
	return [][]DataPatch{s.getPatches()}
}

func (s *ChangefeedReactorState) getPatches() []DataPatch {
	pendingPatches := s.pendingPatches
	s.pendingPatches = nil
	return pendingPatches
}

// CheckCaptureAlive checks if the capture is alive, if the capture offline,
// the etcd worker will exit and throw the ErrLeaseExpired error.
func (s *ChangefeedReactorState) CheckCaptureAlive(captureID model.CaptureID) {
	k := etcd.CDCKey{
		Tp:        etcd.CDCKeyTypeCapture,
		CaptureID: captureID,
	}
	key := k.String()
	patch := &SingleDataPatch{
		Key: util.NewEtcdKey(key),
		Func: func(v []byte) ([]byte, bool, error) {
			// If v is empty, it means that the key-value pair of capture info is not exist.
			// The key-value pair of capture info is written with lease,
			// so if the capture info is not exist, the lease is expired
			if len(v) == 0 {
				return v, false, cerrors.ErrLeaseExpired.GenWithStackByArgs()
			}
			return v, false, nil
		},
	}
	s.pendingPatches = append(s.pendingPatches, patch)
}

// CheckChangefeedNormal checks if the changefeed state is runable,
// if the changefeed status is not runable, the etcd worker will skip all patch of this tick
// the processor should call this function every tick to make sure the changefeed is runable
func (s *ChangefeedReactorState) CheckChangefeedNormal() {
	s.skipPatchesInThisTick = false
	s.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		if info == nil || info.AdminJobType.IsStopState() {
			s.skipPatchesInThisTick = true
			return info, false, cerrors.ErrEtcdTryAgain.GenWithStackByArgs()
		}
		return info, false, nil
	})
	s.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		if status == nil {
			return status, false, nil
		}
		if status.AdminJobType.IsStopState() {
			s.skipPatchesInThisTick = true
			return status, false, cerrors.ErrEtcdTryAgain.GenWithStackByArgs()
		}
		return status, false, nil
	})
}

// PatchInfo appends a DataPatch which can modify the ChangeFeedInfo
func (s *ChangefeedReactorState) PatchInfo(fn func(*model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), changefeedInfoTPI, func(e interface{}) (interface{}, bool, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*model.ChangeFeedInfo))
	})
}

// PatchStatus appends a DataPatch which can modify the ChangeFeedStatus
func (s *ChangefeedReactorState) PatchStatus(fn func(*model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangeFeedStatus,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), changefeedStatusTPI, func(e interface{}) (interface{}, bool, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*model.ChangeFeedStatus))
	})
}

// PatchTaskPosition appends a DataPatch which can modify the TaskPosition of a specified capture
func (s *ChangefeedReactorState) PatchTaskPosition(captureID model.CaptureID, fn func(*model.TaskPosition) (*model.TaskPosition, bool, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskPosition,
		CaptureID:    captureID,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), taskPositionTPI, func(e interface{}) (interface{}, bool, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*model.TaskPosition))
	})
}

var (
	taskPositionTPI     *model.TaskPosition
	changefeedStatusTPI *model.ChangeFeedStatus
	changefeedInfoTPI   *model.ChangeFeedInfo
)

func (s *ChangefeedReactorState) patchAny(key string, tpi interface{}, fn func(interface{}) (interface{}, bool, error)) {
	patch := &SingleDataPatch{
		Key: util.NewEtcdKey(key),
		Func: func(v []byte) ([]byte, bool, error) {
			if s.skipPatchesInThisTick {
				return v, false, cerrors.ErrEtcdIgnore.GenWithStackByArgs()
			}
			var e interface{}
			if v != nil {
				tp := reflect.TypeOf(tpi)
				e = reflect.New(tp.Elem()).Interface()
				err := json.Unmarshal(v, e)
				if err != nil {
					return nil, false, errors.Trace(err)
				}
			}
			ne, changed, err := fn(e)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			if !changed {
				return v, false, nil
			}
			if reflect.ValueOf(ne).IsNil() {
				return nil, true, nil
			}
			nv, err := json.Marshal(ne)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			return nv, true, nil
		},
	}
	s.pendingPatches = append(s.pendingPatches, patch)
}

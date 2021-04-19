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

package model

import (
	"encoding/json"
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"go.uber.org/zap"
)

type GlobalReactorState struct {
	Captures    map[CaptureID]*CaptureInfo
	Changefeeds map[ChangeFeedID]*ChangefeedReactorState
}

// NewGlobalState creates a new global state for processor manager
func NewGlobalState() orchestrator.ReactorState {
	return &GlobalReactorState{
		Captures:    make(map[CaptureID]*CaptureInfo),
		Changefeeds: make(map[ChangeFeedID]*ChangefeedReactorState),
	}
}

func (s *GlobalReactorState) Update(key util.EtcdKey, value []byte, _ bool) error {
	k := new(etcd.CDCKey)
	err := k.Parse(key.String())
	if err != nil {
		return errors.Trace(err)
	}
	if k.Tp == etcd.CDCKeyTypeOwner {
		return nil
	}
	switch k.Tp {
	case etcd.CDCKeyTypeOwner:
		// just skip owner key
		return nil
	case etcd.CDCKeyTypeCapture:
		if value == nil {
			log.Info("Capture deleted",
				zap.String("captureID", k.CaptureID),
				zap.Reflect("old-capture", s.Captures[k.CaptureID]))

			delete(s.Captures, k.CaptureID)
			return nil
		}

		var newCaptureInfo CaptureInfo
		err := newCaptureInfo.Unmarshal(value)
		if err != nil {
			return cerrors.ErrUnmarshalFailed.Wrap(err).GenWithStackByArgs()
		}

		if oldCaptureInfo, ok := s.Captures[k.CaptureID]; ok {
			log.Debug("Capture updated",
				zap.String("captureID", k.CaptureID),
				zap.Reflect("old-capture", oldCaptureInfo),
				zap.Reflect("new-capture", newCaptureInfo))
		} else {
			log.Info("Capture added",
				zap.String("captureID", k.CaptureID),
				zap.Reflect("new-capture", newCaptureInfo))
		}

		s.Captures[k.CaptureID] = &newCaptureInfo
	case etcd.CDCKeyTypeChangefeedInfo,
		etcd.CDCKeyTypeChangeFeedStatus,
		etcd.CDCKeyTypeTaskPosition,
		etcd.CDCKeyTypeTaskStatus,
		etcd.CDCKeyTypeTaskWorkload:
		if len(k.CaptureID) != 0 {
			log.Warn("receive an unexpected etcd event", zap.String("key", key.String()), zap.ByteString("value", value))
			return nil
		}
		changefeedState, exist := s.Changefeeds[k.ChangefeedID]
		if !exist {
			changefeedState = newChangefeedState(k.ChangefeedID)
			s.Changefeeds[k.ChangefeedID] = changefeedState
		}
		if err := changefeedState.UpdateCDCKey(k, value); err != nil {
			return errors.Trace(err)
		}
		if value == nil && !changefeedState.Exist(k.CaptureID) {
			delete(s.Changefeeds, k.ChangefeedID)
		}
	default:
		log.Warn("receive an unexpected etcd event", zap.String("key", key.String()), zap.ByteString("value", value))
	}
	return nil
}

func (s *GlobalReactorState) GetPatches() []*orchestrator.DataPatch {
	var pendingPatches []*orchestrator.DataPatch
	for _, changefeedState := range s.Changefeeds {
		pendingPatches = append(pendingPatches, changefeedState.GetPatches()...)
	}
	return pendingPatches
}

type ChangefeedReactorState struct {
	ID            ChangeFeedID
	Info          *ChangeFeedInfo
	Status        *ChangeFeedStatus
	TaskPositions map[CaptureID]*TaskPosition
	TaskStatuses  map[CaptureID]*TaskStatus
	Workloads     map[CaptureID]TaskWorkload

	pendingPatches []*orchestrator.DataPatch
}

func newChangefeedState(id ChangeFeedID) *ChangefeedReactorState {
	return &ChangefeedReactorState{
		ID:            id,
		TaskPositions: make(map[CaptureID]*TaskPosition),
		TaskStatuses:  make(map[CaptureID]*TaskStatus),
		Workloads:     make(map[CaptureID]TaskWorkload),
	}
}

func (s *ChangefeedReactorState) Update(key util.EtcdKey, value []byte, _ bool) error {
	k := new(etcd.CDCKey)
	err := k.Parse(key.String())
	if err != nil {
		return errors.Trace(err)
	}
	if err := s.UpdateCDCKey(k, value); err != nil {
		log.Error("failed to update status", zap.String("key", key.String()), zap.ByteString("value", value))
		return errors.Trace(err)
	}
	return nil
}

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
		s.Info = new(ChangeFeedInfo)
		e = s.Info
	case etcd.CDCKeyTypeChangeFeedStatus:
		if key.ChangefeedID != s.ID {
			return nil
		}
		if value == nil {
			s.Status = nil
			return nil
		}
		s.Status = new(ChangeFeedStatus)
		e = s.Status
	case etcd.CDCKeyTypeTaskPosition:
		if key.ChangefeedID != s.ID {
			return nil
		}
		if value == nil {
			delete(s.TaskPositions, key.CaptureID)
			return nil
		}
		position := new(TaskPosition)
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
		status := new(TaskStatus)
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
		workload := make(TaskWorkload)
		s.Workloads[key.CaptureID] = workload
		e = workload
	default:
		return nil
	}
	err := json.Unmarshal(value, e)
	if err != nil {
		return errors.Trace(err)
	}
	if key.Tp == etcd.CDCKeyTypeChangefeedInfo {
		err = s.Info.VerifyAndFix()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *ChangefeedReactorState) Exist(captureID CaptureID) bool {
	return s.Info != nil || s.Status != nil || s.TaskPositions[captureID] != nil || s.TaskStatuses[captureID] != nil || s.Workloads[captureID] != nil
}

func (s *ChangefeedReactorState) Active(captureID CaptureID) bool {
	return s.Info != nil && s.Status != nil && s.TaskStatuses[captureID] != nil
}

func (s *ChangefeedReactorState) GetPatches() []*orchestrator.DataPatch {
	pendingPatches := s.pendingPatches
	s.pendingPatches = nil
	return pendingPatches
}

var (
	taskPositionTPI     *TaskPosition
	taskStatusTPI       *TaskStatus
	taskWorkloadTPI     *TaskWorkload
	changefeedStatusTPI *ChangeFeedStatus
	changefeedInfoTPI   *ChangeFeedInfo
)

func (s *ChangefeedReactorState) PatchInfo(fn func(*ChangeFeedInfo) (*ChangeFeedInfo, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), changefeedInfoTPI, func(e interface{}) (interface{}, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*ChangeFeedInfo))
	})
}

func (s *ChangefeedReactorState) PatchStatus(fn func(*ChangeFeedStatus) (*ChangeFeedStatus, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangeFeedStatus,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), changefeedStatusTPI, func(e interface{}) (interface{}, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*ChangeFeedStatus))
	})
}

func (s *ChangefeedReactorState) PatchTaskPosition(captureID CaptureID, fn func(*TaskPosition) (*TaskPosition, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskPosition,
		CaptureID:    captureID,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), taskPositionTPI, func(e interface{}) (interface{}, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*TaskPosition))
	})
}

func (s *ChangefeedReactorState) PatchTaskStatus(captureID CaptureID, fn func(*TaskStatus) (*TaskStatus, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskStatus,
		CaptureID:    captureID,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), taskStatusTPI, func(e interface{}) (interface{}, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*TaskStatus))
	})
}

func (s *ChangefeedReactorState) PatchTaskWorkload(captureID CaptureID, fn func(TaskWorkload) (TaskWorkload, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskWorkload,
		CaptureID:    captureID,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), taskWorkloadTPI, func(e interface{}) (interface{}, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(*e.(*TaskWorkload))
	})
}

func (s *ChangefeedReactorState) patchAny(key string, tpi interface{}, fn func(interface{}) (interface{}, error)) {
	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(key),
		Fun: func(v []byte) ([]byte, error) {
			var e interface{}
			if v != nil {
				tp := reflect.TypeOf(tpi)
				e = reflect.New(tp.Elem()).Interface()
				err := json.Unmarshal(v, e)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			ne, err := fn(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if reflect.ValueOf(ne).IsNil() {
				return nil, nil
			}
			return json.Marshal(ne)
		},
	}
	s.pendingPatches = append(s.pendingPatches, patch)
}

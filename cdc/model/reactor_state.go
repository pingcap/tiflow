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
	"strconv"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"go.uber.org/zap"
)

type GlobalReactorState struct {
	Owner          map[string]struct{}
	Captures       map[CaptureID]*CaptureInfo
	Changefeeds    map[ChangeFeedID]*ChangefeedReactorState
	pendingPatches []orchestrator.DataPatch
}

// NewGlobalState creates a new global state for processor manager
func NewGlobalState() orchestrator.ReactorState {
	return &GlobalReactorState{
		Owner:       map[string]struct{}{},
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
			delete(s.Captures, k.CaptureID)
			return nil
		}

		var newCaptureInfo CaptureInfo
		err := newCaptureInfo.Unmarshal(value)
		if err != nil {
			return cerrors.ErrUnmarshalFailed.Wrap(err).GenWithStackByArgs()
		}

		s.Captures[k.CaptureID] = &newCaptureInfo
	case etcd.CDCKeyTypeChangefeedInfo,
		etcd.CDCKeyTypeChangeFeedStatus,
		etcd.CDCKeyTypeTaskPosition,
		etcd.CDCKeyTypeTaskStatus,
		etcd.CDCKeyTypeTaskWorkload:
		if len(k.CaptureID) == 0 {
			log.Warn("receive an unexpected etcd event", zap.String("key", key.String()), zap.ByteString("value", value))
			return nil
		}
		changefeedState, exist := s.Changefeeds[k.ChangefeedID]
		if !exist {
			if value == nil{
				return nil
			}
			changefeedState = newChangefeedReactorState(k.ChangefeedID)
			s.Changefeeds[k.ChangefeedID] = changefeedState
		}
		if err := changefeedState.UpdateCDCKey(k, value); err != nil {
			return errors.Trace(err)
		}
		if value == nil && !changefeedState.Exist() {
			s.pendingPatches = append(s.pendingPatches, changefeedState.GetPatches()...)
			delete(s.Changefeeds, k.ChangefeedID)
		}
	default:
		log.Warn("receive an unexpected etcd event", zap.String("key", key.String()), zap.ByteString("value", value))
	}
	return nil
}

func (s *GlobalReactorState) GetPatches() []orchestrator.DataPatch {
	pendingPatches := s.pendingPatches
	for _, changefeedState := range s.Changefeeds {
		pendingPatches = append(pendingPatches, changefeedState.GetPatches()...)
	}
	s.pendingPatches = nil
	return pendingPatches
}

func (s *GlobalReactorState) CheckLeaseExpired(leaseID clientv3.LeaseID) {
	k := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeOwner,
		OwnerLeaseID: strconv.FormatInt(int64(leaseID), 10),
	}
	key := k.String()
	patch := &orchestrator.SingleDataPatch{
		Key: util.NewEtcdKey(key),
		Func: func(v []byte) ([]byte, bool, error) {
			if len(v) == 0 {
				return v, false, cerrors.ErrLeaseExpired.GenWithStackByArgs()
			}
			return v, false, nil
		},
	}
	s.pendingPatches = append(s.pendingPatches, patch)
}

type ChangefeedReactorState struct {
	ID            ChangeFeedID
	Info          *ChangeFeedInfo
	Status        *ChangeFeedStatus
	TaskPositions map[CaptureID]*TaskPosition
	TaskStatuses  map[CaptureID]*TaskStatus
	Workloads     map[CaptureID]TaskWorkload

	pendingPatches []orchestrator.DataPatch
}

func newChangefeedReactorState(id ChangeFeedID) *ChangefeedReactorState {
	return &ChangefeedReactorState{
		ID:            id,
		TaskPositions: make(map[CaptureID]*TaskPosition),
		TaskStatuses:  make(map[CaptureID]*TaskStatus),
		Workloads:     make(map[CaptureID]TaskWorkload),
	}
}

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
		e = &workload
	default:
		return nil
	}
	if err := json.Unmarshal(value, e); err != nil {
		return errors.Trace(err)
	}
	if key.Tp == etcd.CDCKeyTypeChangefeedInfo {
		if err := s.Info.VerifyAndFix(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *ChangefeedReactorState) Exist() bool {
	return s.Info != nil || s.Status != nil || len(s.TaskPositions) != 0 || len(s.TaskStatuses) != 0 || len(s.Workloads) != 0
}

func (s *ChangefeedReactorState) Active(captureID CaptureID) bool {
	return s.Info != nil && s.Status != nil && s.TaskStatuses[captureID] != nil
}

func (s *ChangefeedReactorState) GetPatches() []orchestrator.DataPatch {
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

func (s *ChangefeedReactorState) PatchInfo(fn func(*ChangeFeedInfo) (*ChangeFeedInfo, bool, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), changefeedInfoTPI, func(e interface{}) (interface{}, bool, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*ChangeFeedInfo))
	})
}

func (s *ChangefeedReactorState) PatchStatus(fn func(*ChangeFeedStatus) (*ChangeFeedStatus, bool, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangeFeedStatus,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), changefeedStatusTPI, func(e interface{}) (interface{}, bool, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*ChangeFeedStatus))
	})
}

func (s *ChangefeedReactorState) PatchStatusByTaskStatusAndPosition(fn func(status *ChangeFeedStatus,
	taskPositions map[CaptureID]*TaskPosition,
	taskStatuses map[CaptureID]*TaskStatus) (*ChangeFeedStatus, bool, error)) {
	patch := orchestrator.MultiDatePath(func(valueMap map[util.EtcdKey][]byte, changedSet map[util.EtcdKey]struct{}) error {
		// get status and pos
		cdcKey := new(etcd.CDCKey)
		captureIDs := make(map[CaptureID]struct{})
		taskPositions := make(map[CaptureID]*TaskPosition)
		taskStatuses := make(map[CaptureID]*TaskStatus)

		// decode ChangeFeedStatus
		cdcKey.CaptureID = ""
		cdcKey.ChangefeedID = s.ID
		cdcKey.Tp = etcd.CDCKeyTypeChangeFeedStatus
		changefeedStatusKey := util.NewEtcdKey(cdcKey.String())
		changefeedStatusValue := valueMap[changefeedStatusKey]
		var status *ChangeFeedStatus
		if changefeedStatusValue != nil {
			status = new(ChangeFeedStatus)
			err := json.Unmarshal(changefeedStatusValue, status)
			if err != nil {
				return errors.Trace(err)
			}
		}

		// decode TaskPosition and TaskStatus
		for key, value := range valueMap {
			err := cdcKey.Parse(key.String())
			if err != nil {
				return errors.Trace(err)
			}
			if cdcKey.Tp != etcd.CDCKeyTypeCapture {
				continue
			}
			switch cdcKey.Tp {
			case etcd.CDCKeyTypeCapture:
				captureIDs[cdcKey.CaptureID] = struct{}{}
			case etcd.CDCKeyTypeTaskStatus:
				if value == nil {
					continue
				}
				position := new(TaskPosition)
				err := json.Unmarshal(value, position)
				if err != nil {
					return errors.Trace(err)
				}
				taskPositions[cdcKey.CaptureID] = position
			case etcd.CDCKeyTypeTaskPosition:
				if value == nil {
					continue
				}
				taskStatus := new(TaskStatus)
				err := json.Unmarshal(value, taskStatus)
				if err != nil {
					return errors.Trace(err)
				}
				taskStatuses[cdcKey.CaptureID] = taskStatus
			default:
				// do nothing
			}
		}
		status, changed, err := fn(status, taskPositions, taskStatuses)
		if err != nil {
			return errors.Trace(err)
		}
		if !changed {
			return nil
		}
		changefeedStatusValue, err = json.Marshal(status)
		if err != nil {
			return errors.Trace(err)
		}
		valueMap[changefeedStatusKey] = changefeedStatusValue
		changedSet[changefeedStatusKey] = struct{}{}
		return nil
	})
	s.pendingPatches = append(s.pendingPatches, patch)
}

func (s *ChangefeedReactorState) PatchTaskPosition(captureID CaptureID, fn func(*TaskPosition) (*TaskPosition, bool, error)) {
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
		return fn(e.(*TaskPosition))
	})
}

func (s *ChangefeedReactorState) PatchTaskStatus(captureID CaptureID, fn func(*TaskStatus) (*TaskStatus, bool, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskStatus,
		CaptureID:    captureID,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), taskStatusTPI, func(e interface{}) (interface{}, bool, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*TaskStatus))
	})
}

func (s *ChangefeedReactorState) PatchTaskWorkload(captureID CaptureID, fn func(TaskWorkload) (TaskWorkload, bool, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskWorkload,
		CaptureID:    captureID,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), taskWorkloadTPI, func(e interface{}) (interface{}, bool, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(*e.(*TaskWorkload))
	})
}

func (s *ChangefeedReactorState) patchAny(key string, tpi interface{}, fn func(interface{}) (interface{}, bool, error)) {
	patch := &orchestrator.SingleDataPatch{
		Key: util.NewEtcdKey(key),
		Func: func(v []byte) ([]byte, bool, error) {
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

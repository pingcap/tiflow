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

package processor

import (
	"encoding/json"
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"go.uber.org/zap"
)

type globalState struct {
	CaptureID   model.CaptureID
	Changefeeds map[model.ChangeFeedID]*changefeedState
}

// NewGlobalState creates a new global state for processor manager
func NewGlobalState(captureID model.CaptureID) orchestrator.ReactorState {
	return &globalState{
		CaptureID:   captureID,
		Changefeeds: make(map[model.ChangeFeedID]*changefeedState),
	}
}

func (s *globalState) Update(key util.EtcdKey, value []byte, isInit bool) error {
	k := new(etcd.CDCKey)
	err := k.Parse(key.String())
	if err != nil {
		return errors.Trace(err)
	}
	if k.Tp == etcd.CDCKeyTypeCapture || k.Tp == etcd.CDCKeyTypeOnwer {
		return nil
	}
	if len(k.CaptureID) != 0 && k.CaptureID != s.CaptureID {
		return nil
	}
	changefeedState, exist := s.Changefeeds[k.ChangefeedID]
	if !exist {
		changefeedState = newChangeFeedState(k.ChangefeedID, s.CaptureID)
		s.Changefeeds[k.ChangefeedID] = changefeedState
	}
	if err := changefeedState.UpdateCDCKey(k, value); err != nil {
		return errors.Trace(err)
	}
	if value == nil && !changefeedState.Exist() {
		delete(s.Changefeeds, k.ChangefeedID)
	}
	return nil
}

func (s *globalState) GetPatches() []*orchestrator.DataPatch {
	var pendingPatches []*orchestrator.DataPatch
	for _, changefeedState := range s.Changefeeds {
		pendingPatches = append(pendingPatches, changefeedState.GetPatches()...)
	}
	return pendingPatches
}

type changefeedState struct {
	ID           model.ChangeFeedID
	CaptureID    model.CaptureID
	Info         *model.ChangeFeedInfo
	Status       *model.ChangeFeedStatus
	TaskPosition *model.TaskPosition
	TaskStatus   *model.TaskStatus
	Workload     model.TaskWorkload

	pendingPatches []*orchestrator.DataPatch
}

func newChangeFeedState(id model.ChangeFeedID, captureID model.CaptureID) *changefeedState {
	return &changefeedState{
		ID:        id,
		CaptureID: captureID,
	}
}

func (s *changefeedState) Update(key util.EtcdKey, value []byte, isInit bool) error {
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

func (s *changefeedState) UpdateCDCKey(key *etcd.CDCKey, value []byte) error {
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
		if key.ChangefeedID != s.ID || key.CaptureID != s.CaptureID {
			return nil
		}
		if value == nil {
			s.TaskPosition = nil
			return nil
		}
		s.TaskPosition = new(model.TaskPosition)
		e = s.TaskPosition
	case etcd.CDCKeyTypeTaskStatus:
		if key.ChangefeedID != s.ID || key.CaptureID != s.CaptureID {
			return nil
		}
		if value == nil {
			s.TaskStatus = nil
			return nil
		}
		s.TaskStatus = new(model.TaskStatus)
		e = s.TaskStatus
	case etcd.CDCKeyTypeTaskWorkload:
		if key.ChangefeedID != s.ID || key.CaptureID != s.CaptureID {
			return nil
		}
		if value == nil {
			s.Workload = nil
			return nil
		}
		s.Workload = make(model.TaskWorkload)
		e = &s.Workload
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

func (s *changefeedState) Exist() bool {
	return s.Info != nil || s.Status != nil || s.TaskPosition != nil || s.TaskStatus != nil || s.Workload != nil
}

func (s *changefeedState) Active() bool {
	return s.Info != nil && s.Status != nil && s.TaskStatus != nil
}

func (s *changefeedState) GetPatches() []*orchestrator.DataPatch {
	pendingPatches := s.pendingPatches
	s.pendingPatches = nil
	return pendingPatches
}

var (
	taskPositionTPI *model.TaskPosition
	taskStatusTPI   *model.TaskStatus
	taskWorkloadTPI *model.TaskWorkload
)

func (s *changefeedState) PatchTaskPosition(fn func(*model.TaskPosition) (*model.TaskPosition, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskPosition,
		CaptureID:    s.CaptureID,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), taskPositionTPI, func(e interface{}) (interface{}, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*model.TaskPosition))
	})
}

func (s *changefeedState) PatchTaskStatus(fn func(*model.TaskStatus) (*model.TaskStatus, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskStatus,
		CaptureID:    s.CaptureID,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), taskStatusTPI, func(e interface{}) (interface{}, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(e.(*model.TaskStatus))
	})
}

func (s *changefeedState) PatchTaskWorkload(fn func(model.TaskWorkload) (model.TaskWorkload, error)) {
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskWorkload,
		CaptureID:    s.CaptureID,
		ChangefeedID: s.ID,
	}
	s.patchAny(key.String(), taskWorkloadTPI, func(e interface{}) (interface{}, error) {
		// e == nil means that the key is not exist before this patch
		if e == nil {
			return fn(nil)
		}
		return fn(*e.(*model.TaskWorkload))
	})
}

func (s *changefeedState) patchAny(key string, tpi interface{}, fn func(interface{}) (interface{}, error)) {
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

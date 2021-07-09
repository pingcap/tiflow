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

// GlobalReactorState represents a global state which stores all key-value pairs in ETCD
type GlobalReactorState struct {
	Owner          map[string]struct{}
	Captures       map[CaptureID]*CaptureInfo
	Changefeeds    map[ChangeFeedID]*ChangefeedReactorState
	pendingPatches [][]orchestrator.DataPatch
}

// NewGlobalState creates a new global state
func NewGlobalState() orchestrator.ReactorState {
	return &GlobalReactorState{
		Owner:       map[string]struct{}{},
		Captures:    make(map[CaptureID]*CaptureInfo),
		Changefeeds: make(map[ChangeFeedID]*ChangefeedReactorState),
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
			log.Info("remote capture offline", zap.String("capture-id", k.CaptureID))
			delete(s.Captures, k.CaptureID)
			return nil
		}

		var newCaptureInfo CaptureInfo
		err := newCaptureInfo.Unmarshal(value)
		if err != nil {
			return cerrors.ErrUnmarshalFailed.Wrap(err).GenWithStackByArgs()
		}

		log.Info("remote capture online", zap.String("capture-id", k.CaptureID), zap.Any("info", newCaptureInfo))
		s.Captures[k.CaptureID] = &newCaptureInfo
	case etcd.CDCKeyTypeChangefeedInfo,
		etcd.CDCKeyTypeChangeFeedStatus,
		etcd.CDCKeyTypeTaskPosition,
		etcd.CDCKeyTypeTaskStatus,
		etcd.CDCKeyTypeTableStatus,
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
func (s *GlobalReactorState) GetPatches() [][]orchestrator.DataPatch {
	pendingPatches := s.pendingPatches
	for _, changefeedState := range s.Changefeeds {
		pendingPatches = append(pendingPatches, changefeedState.getPatches())
	}
	s.pendingPatches = nil
	return pendingPatches
}

// ChangefeedReactorState represents a changefeed state which stores all key-value pairs of a changefeed in ETCD
type ChangefeedReactorState struct {
	ID            ChangeFeedID
	Info          *ChangeFeedInfo
	Status        *ChangeFeedStatus
	TaskPositions map[CaptureID]*TaskPosition
	TaskStatuses  map[CaptureID]*TaskStatus
	Workloads     map[CaptureID]TaskWorkload

	// Physical statuses represent the actual data stored in Etcd.
	physicalTaskStatuses  map[CaptureID]*TaskStatus
	physicalTableStatuses map[CaptureID]map[TableID]*TableStatus

	// shadowTaskStatuses are temporary data structures used to emulate the in-memory
	// cache of KV pairs in EtcdWorker. They are cleared after each tick.
	shadowTaskStatuses map[CaptureID]*TaskStatus
	// To save computations, shadowTaskStatusesInUse is used to track which CaptureID has a valid
	// shadowTaskStatus.
	shadowTaskStatusesInUse map[CaptureID]struct{}

	pendingPatches        []orchestrator.DataPatch
	skipPatchesInThisTick bool
}

// NewChangefeedReactorState creates a new changefeed reactor state
func NewChangefeedReactorState(id ChangeFeedID) *ChangefeedReactorState {
	return &ChangefeedReactorState{
		ID:            id,
		TaskPositions: make(map[CaptureID]*TaskPosition),
		TaskStatuses:  make(map[CaptureID]*TaskStatus),
		Workloads:     make(map[CaptureID]TaskWorkload),

		physicalTaskStatuses:  make(map[CaptureID]*TaskStatus),
		physicalTableStatuses: make(map[CaptureID]map[TableID]*TableStatus),

		shadowTaskStatuses:      make(map[CaptureID]*TaskStatus),
		shadowTaskStatusesInUse: make(map[CaptureID]struct{}),
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
func (s *ChangefeedReactorState) UpdateCDCKey(key *etcd.CDCKey, value []byte) (err error) {
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
		defer func() {
			if err == nil {
				s.refreshTaskStatusAdminJob(key.CaptureID)
			}
		}()
		if value == nil {
			delete(s.physicalTaskStatuses, key.CaptureID)
			return nil
		}
		status := new(TaskStatus)
		s.physicalTaskStatuses[key.CaptureID] = status
		e = status
	case etcd.CDCKeyTypeTableStatus:
		if key.ChangefeedID != s.ID {
			return nil
		}
		if key.TableID == 0 {
			log.Panic("unexpected tableID 0")
		}
		defer func() {
			if err == nil {
				s.refreshTaskStatus(key.CaptureID, key.TableID)
			}
		}()
		if value == nil {
			if s.physicalTableStatuses[key.CaptureID] == nil {
				log.Panic("unexpected reactor internal state")
			}
			delete(s.physicalTableStatuses[key.CaptureID], key.TableID)
			if len(s.physicalTableStatuses) == 0 {
				s.physicalTableStatuses = nil
			}
			return nil
		}
		if s.physicalTableStatuses[key.CaptureID] == nil {
			s.physicalTableStatuses[key.CaptureID] = make(map[TableID]*TableStatus)
		}
		tableStatus := new(TableStatus)
		s.physicalTableStatuses[key.CaptureID][key.TableID] = tableStatus
		e = tableStatus
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

func (s *ChangefeedReactorState) refreshTaskStatusAdminJob(captureID CaptureID) {
	if s.physicalTaskStatuses[captureID] == nil {
		delete(s.TaskStatuses, captureID)
		return
	}

	if s.TaskStatuses[captureID] == nil {
		s.TaskStatuses[captureID] = new(TaskStatus)
		if len(s.physicalTableStatuses[captureID]) > 0 {
			for tableID := range s.physicalTableStatuses[captureID] {
				s.refreshTaskStatus(captureID, tableID)
			}
		}
	}
	s.TaskStatuses[captureID].AdminJobType = s.physicalTaskStatuses[captureID].AdminJobType
}

func (s *ChangefeedReactorState) refreshTaskStatus(captureID CaptureID, tableID TableID) {
	log.Debug("refreshTaskStatus", zap.String("capture-id", captureID), zap.Int64("table-id", tableID))
	// handle deletion
	if s.physicalTableStatuses[captureID] == nil || s.physicalTableStatuses[captureID][tableID] == nil {
		if s.TaskStatuses[captureID] != nil {
			if s.TaskStatuses[captureID].Tables != nil {
				delete(s.TaskStatuses[captureID].Tables, tableID)
			}
			if s.TaskStatuses[captureID].Operation != nil {
				delete(s.TaskStatuses[captureID].Operation, tableID)
			}
		}
		if len(s.physicalTableStatuses[captureID]) == 0 && s.physicalTaskStatuses[captureID] == nil {
			delete(s.TaskStatuses, captureID)
		}
		return
	}
	tableStatus := s.physicalTableStatuses[captureID][tableID]
	if s.TaskStatuses[captureID] == nil {
		s.TaskStatuses[captureID] = new(TaskStatus)
	}
	if tableStatus.ReplicaInfo != nil {
		if s.TaskStatuses[captureID].Tables == nil {
			s.TaskStatuses[captureID].Tables = make(map[TableID]*TableReplicaInfo)
		}
		s.TaskStatuses[captureID].Tables[tableID] = tableStatus.ReplicaInfo.Clone()
	} else {
		if s.TaskStatuses[captureID].Tables != nil {
			delete(s.TaskStatuses[captureID].Tables, tableID)
		}
	}
	if tableStatus.Operation != nil {
		if s.TaskStatuses[captureID].Operation == nil {
			s.TaskStatuses[captureID].Operation = make(map[TableID]*TableOperation)
		}
		s.TaskStatuses[captureID].Operation[tableID] = tableStatus.Operation.Clone()
	} else {
		if s.TaskStatuses[captureID].Operation != nil {
			delete(s.TaskStatuses[captureID].Operation, tableID)
		}
	}
}

// Exist returns false if all keys of this changefeed in ETCD is not exist
func (s *ChangefeedReactorState) Exist() bool {
	return s.Info != nil || s.Status != nil || len(s.TaskPositions) != 0 || len(s.TaskStatuses) != 0 || len(s.Workloads) != 0
}

// Active return true if the changefeed is ready to be processed
func (s *ChangefeedReactorState) Active(captureID CaptureID) bool {
	return s.Info != nil && s.Status != nil && s.TaskStatuses[captureID] != nil
}

// GetPatches implements the ReactorState interface
func (s *ChangefeedReactorState) GetPatches() [][]orchestrator.DataPatch {
	return [][]orchestrator.DataPatch{s.getPatches()}
}

func (s *ChangefeedReactorState) getPatches() []orchestrator.DataPatch {
	pendingPatches := s.pendingPatches
	s.pendingPatches = nil
	if len(s.shadowTaskStatusesInUse) != 0 {
		s.shadowTaskStatusesInUse = make(map[CaptureID]struct{})
		s.shadowTaskStatuses = make(map[CaptureID]*TaskStatus)
	}
	return pendingPatches
}

// CheckCaptureAlive checks if the capture is alive, if the capture offline,
// the etcd worker will exit and throw the ErrLeaseExpired error.
func (s *ChangefeedReactorState) CheckCaptureAlive(captureID CaptureID) {
	k := etcd.CDCKey{
		Tp:        etcd.CDCKeyTypeCapture,
		CaptureID: captureID,
	}
	key := k.String()
	patch := &orchestrator.SingleDataPatch{
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
	s.PatchInfo(func(info *ChangeFeedInfo) (*ChangeFeedInfo, bool, error) {
		if info == nil || info.AdminJobType.IsStopState() {
			s.skipPatchesInThisTick = true
			return info, false, cerrors.ErrEtcdTryAgain.GenWithStackByArgs()
		}
		return info, false, nil
	})
	s.PatchStatus(func(status *ChangeFeedStatus) (*ChangeFeedStatus, bool, error) {
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

// PatchStatus appends a DataPatch which can modify the ChangeFeedStatus
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

// PatchTaskPosition appends a DataPatch which can modify the TaskPosition of a specified capture
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

// PatchTaskStatus appends a DataPatch which can modify the TaskStatus of a specified capture
func (s *ChangefeedReactorState) PatchTaskStatus(captureID CaptureID, fn func(*TaskStatus) (*TaskStatus, bool, error)) {
	if _, ok := s.shadowTaskStatusesInUse[captureID]; !ok {
		if s.TaskStatuses[captureID] != nil {
			s.shadowTaskStatuses[captureID] = s.TaskStatuses[captureID].Clone()
		}
		s.shadowTaskStatusesInUse[captureID] = struct{}{}
	}
	pre := s.shadowTaskStatuses[captureID]
	var preCloned *TaskStatus
	if pre != nil {
		preCloned = pre.Clone()
	}
	post, changed, err := fn(pre)
	pre = preCloned
	s.shadowTaskStatuses[captureID] = post
	if err != nil {
		// Throws error in this complicated way for backward compatibility
		key := &etcd.CDCKey{
			Tp:           etcd.CDCKeyTypeTaskStatus,
			CaptureID:    captureID,
			ChangefeedID: s.ID,
		}
		patch := &orchestrator.SingleDataPatch{
			Key: util.NewEtcdKey(key.String()),
			Func: func(v []byte) ([]byte, bool, error) {
				return nil, false, err
			},
		}
		s.pendingPatches = append(s.pendingPatches, patch)
		return
	}

	if !changed {
		return
	}

	if (pre == nil && post != nil) || (pre != nil && (post == nil || post.AdminJobType != pre.AdminJobType)) {
		key := &etcd.CDCKey{
			Tp:           etcd.CDCKeyTypeTaskStatus,
			CaptureID:    captureID,
			ChangefeedID: s.ID,
		}
		s.patchAny(key.String(), taskStatusTPI, func(i interface{}) (interface{}, bool, error) {
			if post == nil {
				return post, pre != nil, nil
			}
			if pre == nil {
				return &TaskStatus{
					AdminJobType: post.AdminJobType,
				}, true, nil
			}
			pre := i.(*TaskStatus)
			changed := pre.AdminJobType != post.AdminJobType
			pre.AdminJobType = post.AdminJobType
			return pre, changed, nil
		})
	}

	updatedTableStatuses := make(map[TableID]struct{})
	if pre != nil {
		// Checks for updates and deletions
		for tableID, preStatus := range pre.Tables {
			if post == nil || post.Tables == nil || post.Tables[tableID] == nil {
				// deleted
				updatedTableStatuses[tableID] = struct{}{}
				continue
			}
			if post.Tables[tableID].StartTs != preStatus.StartTs || post.Tables[tableID].MarkTableID != preStatus.MarkTableID {
				// updated
				updatedTableStatuses[tableID] = struct{}{}
				continue
			}
		}
		for tableID, preOperation := range pre.Operation {
			if post == nil || post.Operation == nil || post.Operation[tableID] == nil {
				// deleted
				updatedTableStatuses[tableID] = struct{}{}
				continue
			}
			if post.Operation[tableID].Delete != preOperation.Delete ||
				post.Operation[tableID].Done != preOperation.Done ||
				post.Operation[tableID].Status != preOperation.Status ||
				post.Operation[tableID].BoundaryTs != preOperation.BoundaryTs ||
				post.Operation[tableID].Flag != preOperation.Flag {

				// updated
				updatedTableStatuses[tableID] = struct{}{}
				continue
			}
		}
	}

	if post != nil {
		// Checks for additions
		for tableID := range post.Tables {
			if pre == nil || pre.Tables == nil {
				updatedTableStatuses[tableID] = struct{}{}
				continue
			}
			if _, ok := pre.Tables[tableID]; !ok {
				updatedTableStatuses[tableID] = struct{}{}
			}
		}
		for tableID := range post.Operation {
			if pre == nil || pre.Operation == nil {
				updatedTableStatuses[tableID] = struct{}{}
				continue
			}
			if _, ok := pre.Operation[tableID]; !ok {
				updatedTableStatuses[tableID] = struct{}{}
			}
		}
	}

	for tableID := range updatedTableStatuses {
		tableID := tableID
		key := &etcd.CDCKey{
			Tp:           etcd.CDCKeyTypeTableStatus,
			ChangefeedID: s.ID,
			CaptureID:    captureID,
			TableID:      tableID,
		}
		s.patchAny(key.String(), tableStatusTPI, func(i interface{}) (interface{}, bool, error) {
			var preStatus *TableStatus
			if i != nil {
				preStatus = i.(*TableStatus)
			}

			postStatus := new(TableStatus)
			if post != nil && post.Tables != nil && post.Tables[tableID] != nil {
				postStatus.ReplicaInfo = post.Tables[tableID].Clone()
			}
			if post != nil && post.Operation != nil && post.Operation[tableID] != nil {
				postStatus.Operation = post.Operation[tableID].Clone()
			}
			if postStatus.Operation == nil && postStatus.ReplicaInfo == nil {
				postStatus = nil
			}
			return postStatus, !reflect.DeepEqual(postStatus, preStatus), nil
		})
	}
}

// PatchTaskWorkload appends a DataPatch which can modify the TaskWorkload of a specified capture
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

var (
	taskPositionTPI     *TaskPosition
	taskStatusTPI       *TaskStatus
	tableStatusTPI      *TableStatus
	taskWorkloadTPI     *TaskWorkload
	changefeedStatusTPI *ChangeFeedStatus
	changefeedInfoTPI   *ChangeFeedInfo
)

func (s *ChangefeedReactorState) patchAny(key string, tpi interface{}, fn func(interface{}) (interface{}, bool, error)) {
	patch := &orchestrator.SingleDataPatch{
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

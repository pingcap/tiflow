package processor

import (
	"encoding/json"
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

type globalState struct {
	CaptureID            model.CaptureID
	Changefeeds          map[model.ChangeFeedID]changefeedState
	removedChangefeedIDs []model.ChangeFeedID
}

func NewGlobalState(captureID model.CaptureID) orchestrator.ReactorState {
	return &globalState{
		CaptureID:   captureID,
		Changefeeds: make(map[model.ChangeFeedID]changefeedState),
	}
}

func (s *globalState) Update(key util.EtcdKey, value []byte) error {
	k := new(CDCEtcdKey)
	err := k.Parse(key.String())
	if err != nil {
		return errors.Trace(err)
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
		s.removedChangefeedIDs = append(s.removedChangefeedIDs, k.ChangefeedID)
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

func (s *globalState) GetRemovedChangefeeds() (removedChangefeedIDs []model.ChangeFeedID) {
	removedChangefeedIDs = s.removedChangefeedIDs
	s.removedChangefeedIDs = nil
	return
}

type changefeedState struct {
	id           model.ChangeFeedID
	captureID    model.CaptureID
	info         *model.ChangeFeedInfo
	status       *model.ChangeFeedStatus
	taskPosition *model.TaskPosition
	taskStatus   *model.TaskStatus
	workload     model.TaskWorkload

	pendingPatches []*orchestrator.DataPatch
}

func newChangeFeedState(id model.ChangeFeedID, captureID model.CaptureID) changefeedState {
	return changefeedState{
		id:        id,
		captureID: captureID,
	}
}

func (s changefeedState) Update(key util.EtcdKey, value []byte) error {
	k := new(CDCEtcdKey)
	err := k.Parse(key.String())
	if err != nil {
		return errors.Trace(err)
	}
	return s.UpdateCDCKey(k, value)
}

func (s changefeedState) UpdateCDCKey(key *CDCEtcdKey, value []byte) error {
	var e interface{}
	switch key.Tp {
	case CDCEtcdKeyTypeChangefeedInfo:
		if key.ChangefeedID != s.id {
			return nil
		}
		if value == nil {
			s.info = nil
			return nil
		}
		if s.info == nil {
			s.info = new(model.ChangeFeedInfo)
		}
		e = s.info
	case CDCEtcdKeyTypeChangeFeedStatus:
		if key.ChangefeedID != s.id {
			return nil
		}
		if value == nil {
			s.status = nil
			return nil
		}
		if s.status == nil {
			s.status = new(model.ChangeFeedStatus)
		}
		e = s.status
	case CDCEtcdKeyTypeTaskPosition:
		if key.ChangefeedID != s.id || key.CaptureID != s.captureID {
			return nil
		}
		if value == nil {
			s.taskPosition = nil
			return nil
		}
		if s.taskPosition == nil {
			s.taskPosition = new(model.TaskPosition)
		}
		e = s.taskPosition
	case CDCEtcdKeyTypeTaskStatus:
		if key.ChangefeedID != s.id || key.CaptureID != s.captureID {
			return nil
		}
		if value == nil {
			s.taskStatus = nil
			return nil
		}
		if s.taskPosition == nil {
			s.taskStatus = new(model.TaskStatus)
		}
		e = s.taskStatus
	case CDCEtcdKeyTypeTaskWorkload:
		if key.ChangefeedID != s.id || key.CaptureID != s.captureID {
			return nil
		}
		if value == nil {
			s.workload = nil
			return nil
		}
		if s.workload == nil {
			s.workload = make(model.TaskWorkload)
		}
		e = s.workload
	default:
		return nil
	}
	err := json.Unmarshal(value, e)
	if err != nil {
		return errors.Trace(err)
	}
	if key.Tp == CDCEtcdKeyTypeChangefeedInfo {
		err = s.info.VerifyAndFix()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s changefeedState) Exist() bool {
	return s.info != nil || s.status != nil || s.taskPosition != nil || s.taskStatus != nil || s.workload != nil
}

func (s changefeedState) Active() bool {
	return s.info != nil && s.status == nil && s.taskStatus != nil
}

func (s changefeedState) GetPatches() []*orchestrator.DataPatch {
	pendingPatches := s.pendingPatches
	s.pendingPatches = nil
	return pendingPatches
}

var taskPositionTPI *model.TaskPosition
var taskStatusTPI *model.TaskStatus
var taskWorkloadTPI model.TaskWorkload

func (s changefeedState) PatchTaskPosition(fn func(*model.TaskPosition) (*model.TaskPosition, error)) {
	key := &CDCEtcdKey{
		Tp:           CDCEtcdKeyTypeTaskPosition,
		CaptureID:    s.captureID,
		ChangefeedID: s.id,
	}
	s.patchAny(key.String(), taskPositionTPI, func(e interface{}) (interface{}, error) {
		return fn(e.(*model.TaskPosition))
	})
}

func (s changefeedState) PatchTaskStatus(fn func(*model.TaskStatus) (*model.TaskStatus, error)) {
	key := &CDCEtcdKey{
		Tp:           CDCEtcdKeyTypeTaskStatus,
		CaptureID:    s.captureID,
		ChangefeedID: s.id,
	}
	s.patchAny(key.String(), taskStatusTPI, func(e interface{}) (interface{}, error) {
		return fn(e.(*model.TaskStatus))
	})
}

func (s changefeedState) PatchTaskWorkload(fn func(model.TaskWorkload) (model.TaskWorkload, error)) {
	key := &CDCEtcdKey{
		Tp:           CDCEtcdKeyTypeTaskWorkload,
		CaptureID:    s.captureID,
		ChangefeedID: s.id,
	}
	s.patchAny(key.String(), taskWorkloadTPI, func(e interface{}) (interface{}, error) {
		return fn(e.(model.TaskWorkload))
	})
}

func (s changefeedState) patchAny(key string, tpi interface{}, fn func(interface{}) (interface{}, error)) {
	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(key),
		Fun: func(v []byte) ([]byte, error) {
			var e interface{}
			if v != nil {
				tp := reflect.TypeOf(tpi)
				if tp.Kind() != reflect.Ptr {
					return nil, errors.Errorf("expected pointer type, got %T", tpi)
				}
				e := reflect.New(tp.Elem()).Interface()
				err := json.Unmarshal(v, e)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			ne, err := fn(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if ne == nil {
				return nil, nil
			} else {
				return json.Marshal(ne)
			}
		},
	}
	s.pendingPatches = append(s.pendingPatches, patch)
}

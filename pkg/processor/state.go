package processor

import (
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

type processorState struct {
	CaptureID   model.CaptureID
	Changefeeds map[model.ChangeFeedID]changefeedState
}

func newProcessorState(captureID model.CaptureID) *processorState {
	return &processorState{
		CaptureID:   captureID,
		Changefeeds: make(map[model.ChangeFeedID]changefeedState),
	}
}

func (p *processorState) Update(key util.EtcdKey, value []byte) error {
	k := new(CDCEtcdKey)
	err := k.Parse(key.String())
	if err != nil {
		return errors.Trace(err)
	}
	if len(k.CaptureID) != 0 && k.CaptureID != p.CaptureID {
		return nil
	}
	changefeedState, exist := p.Changefeeds[k.ChangefeedID]
	if !exist {
		changefeedState = newChangeFeedState(k.ChangefeedID, p.CaptureID)
		p.Changefeeds[k.ChangefeedID] = changefeedState
	}
	return changefeedState.UpdateCDCKey(k, value)
}

func (p *processorState) GetPatches() []*orchestrator.DataPatch {
	var pendingPatches []*orchestrator.DataPatch
	for _, changefeedState := range p.Changefeeds {
		pendingPatches = append(pendingPatches, changefeedState.GetPatches()...)
	}
	return pendingPatches
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
		id:           id,
		captureID:    captureID,
		info:         new(model.ChangeFeedInfo),
		status:       new(model.ChangeFeedStatus),
		taskPosition: new(model.TaskPosition),
		taskStatus:   new(model.TaskStatus),
		workload:     make(model.TaskWorkload),
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
		e = s.info
	case CDCEtcdKeyTypeChangeFeedStatus:
		if key.ChangefeedID != s.id {
			return nil
		}
		e = s.status
	case CDCEtcdKeyTypeTaskPosition:
		if key.ChangefeedID != s.id || key.CaptureID != s.captureID {
			return nil
		}
		e = s.taskPosition
	case CDCEtcdKeyTypeTaskStatus:
		if key.ChangefeedID != s.id || key.CaptureID != s.captureID {
			return nil
		}
		e = s.taskStatus
	case CDCEtcdKeyTypeTaskWorkload:
		if key.ChangefeedID != s.id || key.CaptureID != s.captureID {
			return nil
		}
		e = s.workload
	default:
		return nil
	}
	return json.Unmarshal(value, e)
}

func (s changefeedState) GetPatches() []*orchestrator.DataPatch {
	pendingPatches := s.pendingPatches
	s.pendingPatches = nil
	return pendingPatches
}

func (s changefeedState) PatchTaskPosition(fn func(*model.TaskPosition) error) {
	key := &CDCEtcdKey{
		Tp:           CDCEtcdKeyTypeTaskPosition,
		CaptureID:    s.captureID,
		ChangefeedID: s.id,
	}
	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(key.String()),
		Fun: func(v []byte) ([]byte, error) {
			e := new(model.TaskPosition)
			err := json.Unmarshal(v, e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = fn(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return json.Marshal(e)
		},
	}
	s.pendingPatches = append(s.pendingPatches, patch)
}

func (s changefeedState) PatchTaskStatus(fn func(*model.TaskStatus) error) {
	key := &CDCEtcdKey{
		Tp:           CDCEtcdKeyTypeTaskStatus,
		CaptureID:    s.captureID,
		ChangefeedID: s.id,
	}
	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(key.String()),
		Fun: func(v []byte) ([]byte, error) {
			e := new(model.TaskStatus)
			err := json.Unmarshal(v, e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = fn(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return json.Marshal(e)
		},
	}
	s.pendingPatches = append(s.pendingPatches, patch)
}

func (s changefeedState) PatchTaskWorkload(fn func(model.TaskWorkload) error) {
	key := &CDCEtcdKey{
		Tp:           CDCEtcdKeyTypeTaskWorkload,
		CaptureID:    s.captureID,
		ChangefeedID: s.id,
	}
	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(key.String()),
		Fun: func(v []byte) ([]byte, error) {
			e := make(model.TaskWorkload)
			err := json.Unmarshal(v, e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = fn(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return json.Marshal(e)
		},
	}
	s.pendingPatches = append(s.pendingPatches, patch)
}

//func (s *changefeedState) ReportError(err error) {
//	// TODO add a log here
//	// record error information in etcd
//	var code string
//	if terror, ok := err.(*errors.Error); ok {
//		code = string(terror.RFCCode())
//	} else {
//		code = string(cerror.ErrProcessorUnknown.RFCCode())
//	}
//	processor.position.Error = &model.RunningError{
//		Addr:    captureInfo.AdvertiseAddr,
//		Code:    code,
//		Message: err.Error(),
//	}
//}

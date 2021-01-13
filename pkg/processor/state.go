package processor

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

type processorState struct {
	ID          model.CaptureID
	Changefeeds map[model.ChangeFeedID]changefeedState
}

func newProcessorState(captureID model.CaptureID) *processorState {
	return &processorState{
		ID:          captureID,
		Changefeeds: make(map[model.ChangeFeedID]changefeedState),
	}
}

func (p *processorState) Update(key util.EtcdKey, value []byte) error {
	panic("implement me")
}

func (p *processorState) GetPatches() []*orchestrator.DataPatch {

}

type changefeedState struct {
	changefeedStatus *model.ChangeFeedStatus
	taskPosition     *model.TaskPosition
	taskStatus       *model.TaskStatus
	workload         model.TaskWorkload
}

func (s *changefeedState) Update(key util.EtcdKey, value []byte) error {
	panic("implement me")
}

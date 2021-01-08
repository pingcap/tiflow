package processor

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

type processorState struct {
	ID                 model.CaptureID
	ChangefeedStatuses map[model.ChangeFeedID]*model.ChangeFeedStatus
	TaskPositions      map[model.ChangeFeedID]*model.TaskPosition
	TaskStatuses       map[model.ChangeFeedID]*model.TaskStatus
	Workloads          map[model.ChangeFeedID]*model.WorkloadInfo
}

func newProcessorState(captureID model.CaptureID) *processorState {
	return &processorState{
		ID:                 captureID,
		ChangefeedStatuses: make(map[model.ChangeFeedID]*model.ChangeFeedStatus),
		TaskPositions:      make(map[model.ChangeFeedID]*model.TaskPosition),
		TaskStatuses:       make(map[model.ChangeFeedID]*model.TaskStatus),
		Workloads:          make(map[model.ChangeFeedID]*model.WorkloadInfo),
	}
}

func (p *processorState) Update(key util.EtcdKey, value []byte) error {
	panic("implement me")
}

func (p *processorState) GetPatches() []*orchestrator.DataPatch {
	panic("implement me")
}

package processor

import "github.com/pingcap/ticdc/cdc/model"

type processorState struct {
	Owner              model.CaptureID
	Captures           map[model.CaptureID]*model.CaptureInfo
	ChangefeedStatuses map[model.ChangeFeedID]*model.ChangeFeedStatus
	TaskPositions      map[model.ChangeFeedID]map[model.CaptureID]*model.TaskPosition
	TaskStatuses       map[model.ChangeFeedID]map[model.CaptureID]*model.TaskStatus
}

package scheduler

import "github.com/pingcap/tiflow/cdc/model"

const (
	// captureIDNotDraining is a placeholder for no capture is draining now.
	captureIDNotDraining       = ""
	drainCaptureTableBatchSize = 1
	drainCaptureRelaxTicks     = 10
)

type drainCaptureController interface {
	Drain(target model.CaptureID) error
}

func newDrainCaptureController() drainCaptureController {
	return &drainCaptureControllerImpl{}
}

type drainCaptureControllerImpl struct {
	drainTarget model.CaptureID
}

func (c *drainCaptureControllerImpl) Drain(target model.CaptureID) error {
	c.drainTarget = target

	return nil
}

func (c *drainCaptureControllerImpl) moveTable(tableID model.TableID, destination *model.CaptureID) error {
	return nil
}

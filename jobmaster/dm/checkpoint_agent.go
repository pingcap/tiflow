package dm

import (
	"context"

	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/lib"
)

type CheckpointAgentImpl struct{}

func (c *CheckpointAgentImpl) IsFresh(ctx context.Context, workerType lib.WorkerType, taskCfg *metadata.Task) (bool, error) {
	return true, nil
}

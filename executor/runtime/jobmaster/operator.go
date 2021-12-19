package jobmaster

import (
	"context"

	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/master/jobmaster/system"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type jobMasterAgent struct {
	master system.JobMaster
}

func (j *jobMasterAgent) Prepare() error {
	return j.master.Start(context.Background())
}

func (j *jobMasterAgent) Next(_ *runtime.TaskContext, _ *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	// TODO: Do something such as monitor the status of jobmaster.
	return nil, false, nil
}

func (j *jobMasterAgent) NextWantedInputIdx() int { return runtime.DontNeedData }

func (j *jobMasterAgent) Close() error {
	// "Stop" should not be blocked
	go func() {
		err := j.master.Stop(context.Background())
		log.L().Info("finish stop job", zap.Int64("id", int64(j.master.ID())), zap.Error(err))
	}()
	return nil
}

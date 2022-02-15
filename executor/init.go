package executor

import (
	cvstask "github.com/hanfei1991/microcosm/executor/cvsTask"
	cvs "github.com/hanfei1991/microcosm/jobmaster/cvsJob"
	"github.com/hanfei1991/microcosm/lib/registry"
)

func init() {
	cvstask.RegisterWorker()
	cvs.RegisterWorker()
	registry.RegisterFake(registry.GlobalWorkerRegistry())
}

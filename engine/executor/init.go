package executor

import (
	_ "github.com/hanfei1991/microcosm/dm" // register dm
	cvstask "github.com/hanfei1991/microcosm/executor/cvsTask"
	cvs "github.com/hanfei1991/microcosm/jobmaster/cvsJob"
	"github.com/hanfei1991/microcosm/jobmaster/dm"
	"github.com/hanfei1991/microcosm/lib/registry"
)

func init() {
	cvstask.RegisterWorker()
	cvs.RegisterWorker()
	dm.RegisterWorker()
	registry.RegisterFake(registry.GlobalWorkerRegistry())
}

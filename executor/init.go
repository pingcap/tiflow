package executor

import (
	cvstask "github.com/hanfei1991/microcosm/executor/cvsTask"
	cvs "github.com/hanfei1991/microcosm/jobmaster/cvsJob"
)

func init() {
	cvstask.RegisterWorker()
	cvs.RegisterWorker()
}

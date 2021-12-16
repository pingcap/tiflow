package executor

import (
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/executor/runtime/benchmark"
	"github.com/hanfei1991/microcosm/executor/runtime/jobmaster"
)

func init() {
	// register operator builder for runtime
	runtime.InitOpBuilders()
	benchmark.RegisterBuilder()
	jobmaster.RegisterBuilder()
}

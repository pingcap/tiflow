package executor

import (
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/executor/runtime/benchmark"
)

func init() {
	// register operator builder for runtime
	runtime.InitOpBuilders()
	benchmark.RegisterBuilder()
}

package util

import (
	"github.com/pingcap/failpoint"
)

var FailpointBuild = failpointBuild()

// using failpoint package whether the failpoint is enabled to avoid imported and not used error
var __failpointValue = failpoint.Value(0)

func failpointBuild() bool {
	failpoint.Return(true)
	return false
}

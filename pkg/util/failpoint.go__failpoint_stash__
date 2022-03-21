// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"github.com/pingcap/failpoint"
)

// FailpointBuild is ture if this is a failpoint build
var FailpointBuild = isFailpointBuild()

// using failpoint package when the failpoint is enabled to avoid imported and not used error
var _failpointValue = failpoint.Value(0) //nolint

func isFailpointBuild() bool {
	failpoint.Return(true)
	return false
}

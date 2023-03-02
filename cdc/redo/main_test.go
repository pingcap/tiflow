// Copyright 2022 PingCAP, Inc.
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

package redo

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/leakutil"
)

func TestMain(m *testing.M) {
<<<<<<< HEAD
	originValue := defaultGCIntervalInMs
	defaultGCIntervalInMs = 1
	defer func() {
		defaultGCIntervalInMs = originValue
	}()

=======
>>>>>>> 8430a081f2 (redo(ticdc): add unit tests for redo meta manager (#8363))
	leakutil.SetUpLeakTest(m)
}

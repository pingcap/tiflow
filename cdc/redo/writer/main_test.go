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

package writer

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/leakutil"
)

func TestMain(m *testing.M) {
	originValue := defaultGCIntervalInMs
	defaultGCIntervalInMs = 1
	defer func() {
		defaultGCIntervalInMs = originValue
	}()

	leakutil.SetUpLeakTest(m)
}

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

package leakutil

import (
	"testing"

	"go.uber.org/goleak"
)

// SetUpLeakTest ignore unexpected common etcd and opencensus stack functions for goleak
// options can be used to implement other ignore items
func SetUpLeakTest(m *testing.M, options ...goleak.Option) {
	opts := []goleak.Option{
		// Can add any ignorable goroutine, such as
		// goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
	}

	opts = append(opts, options...)

	goleak.VerifyTestMain(m, opts...)
}

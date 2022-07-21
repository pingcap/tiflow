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
package client

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/leakutil"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		// TODO: remove the following lines after executor manager supports to
		// remove failed clients. Otherwise test case TestAddNonExistentExecutor
		// will fail.
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
	}
	leakutil.SetUpLeakTest(m, opts...)
}

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

func TestSetUpLeakTest(t *testing.T) {
	leakChan := make(chan interface{})

	go func() {
		<-leakChan
	}()
}

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/pingcap/tiflow/pkg/leakutil.TestSetUpLeakTest.func1"),
	}

	SetUpLeakTest(m, opts...)
}

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

package test

import (
	"go.uber.org/atomic"
)

// globalTestFlag indicates if this program is in test mode.
// If so, we use mock-grpc rather than a real one.
var globalTestFlag = *atomic.NewBool(false)

// GetGlobalTestFlag returns the value of global test flag
func GetGlobalTestFlag() bool {
	return globalTestFlag.Load()
}

// SetGlobalTestFlag sets global test flag to given value
func SetGlobalTestFlag(val bool) {
	globalTestFlag.Store(val)
}

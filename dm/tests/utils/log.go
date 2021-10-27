// Copyright 2019 PingCAP, Inc.
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

package utils

import (
	"os"

	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
)

// ExitWithError forces to exist the process, it's often used in integration tests.
func ExitWithError(err error) {
	log.L().Error("", zap.Error(err))
	os.Exit(1)
}

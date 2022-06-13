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

package rpcerror

import (
	"testing"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type testError struct {
	Error[Retryable, Unavailable]

	Val string `json:"val"`
}

func TestNormalize(t *testing.T) {
	testErrorPrototype := Normalize[testError](WithName("ErrTestError"), WithMessage("test message"))
	err := testErrorPrototype.GenWithStack(&testError{Val: "first test error"})
	log.Info("error", zap.Error(err))
}

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

package workerpoolv2

import (
	"context"

	"github.com/pingcap/tiflow/pkg/workerpoolv2/internal"
)

// Pool is a workerpool that runs a large number of handles.
type Pool interface {
	Run(ctx context.Context) error
	RegisterHandle(handle internal.Poller)
}

// NewPool creates a new Pool with the given workerNum.
func NewPool(workerNum int) Pool {
	return internal.NewPoolImpl(workerNum)
}

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

package resourcejob

import (
	"encoding/json"

	"github.com/pingcap/errors"
)

type workerState string

const (
	workerStateGenerating = workerState("generating")
	workerStateSorting    = workerState("sorting")
	workerStateCommitting = workerState("committing")
	workerStateFinished   = workerState("finished")
)

type workerStatus struct {
	State workerState
}

func newWorkerStatusFromBytes(bytesData []byte) (*workerStatus, error) {
	ret := new(workerStatus)
	if err := json.Unmarshal(bytesData, ret); err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

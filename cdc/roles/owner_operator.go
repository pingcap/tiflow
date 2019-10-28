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

package roles

import (
	"math"

	"github.com/pingcap/parser/model"
)

type ddlHandler struct {
}

func NewDDLHandler() *ddlHandler {
	return &ddlHandler{}
}

func (h *ddlHandler) PullDDL() (uint64, []*model.Job, error) {
	return math.MaxUint64, nil, nil
}

func (h *ddlHandler) ExecDDL(*model.Job) error {
	return nil
}

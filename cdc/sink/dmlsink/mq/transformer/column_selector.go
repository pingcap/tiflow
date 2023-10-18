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

package transformer

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
)

type Transformer interface {
	Transform(event *model.RowChangedEvent) error
}

type columnSelector struct {
}

// NewColumnSelector return a new columnSelector.
func NewColumnSelector(replicaConfig *config.ReplicaConfig) (*columnSelector, error) {
	//rules []config.ColumnSelector
	return &columnSelector{}, nil
}

// Transform implements Transformer interface.
func (s *columnSelector) Transform(event *model.RowChangedEvent) error {
	// caution: after filter out columns, original columns should still keep at the same offset
	// to prevent column dispatcher visit wrong column data.
	return nil
}

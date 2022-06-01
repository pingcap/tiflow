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

package v2

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/security"
)

// Tso contains timestamp get from PD
type Tso struct {
	Timestamp int64 `json:"timestamp"`
	LogicTime int64 `json:"logic-time"`
}

// Tables contains IneligibleTables and EligibleTables
type Tables struct {
	IneligibleTables []model.TableName `json:"ineligible-tables,omitempty"`
	EligibleTables   []model.TableName `json:"eligible-tables,omitempty"`
}

type VerifyTableConfig struct {
	PDAddrs       []string              `json:"pd-addrs"`
	Credential    *security.Credential  `json:"credential"`
	ReplicaConfig *config.ReplicaConfig `json:"replica-config"`
	StartTs       uint64                `json:"start-ts"`
}

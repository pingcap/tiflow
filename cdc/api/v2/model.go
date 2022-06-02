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
)

// Tso contains timestamp get from PD
type Tso struct {
	Timestamp int64 `json:"timestamp"`
	LogicTime int64 `json:"logic-time"`
}

type ChangefeedConfig struct {
	Namespace string `json:"namespace"`
	ID        string `json:"changefeed_id"`
	StartTS   uint64 `json:"start_ts"`
	TargetTS  uint64 `json:"target_ts"`
	SinkURI   string `json:"sink_uri"`
	// timezone used when checking sink uri
	TimeZone      string               `json:"timezone" default:"system"`
	ReplicaConfig config.ReplicaConfig `json:"replica-config"`
	Opts          map[string]string    `json:"opts"`
	Engine        model.SortEngine     `json:"sort-engine"`
}

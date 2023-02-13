// Copyright 2023 PingCAP, Inc.
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
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
)

// MetricsCollector is the kafka metrics collector based on kafka-go library.
type MetricsCollector struct {
	changefeedID model.ChangeFeedID
	role         util.Role
	admin        kafka.ClusterAdminClient
}

// NewMetricsCollector return a kafka metrics collector
func NewMetricsCollector(
	changefeedID model.ChangeFeedID,
	role util.Role,
	admin kafka.ClusterAdminClient,
) *MetricsCollector {
	return &MetricsCollector{
		changefeedID: changefeedID,
		role:         role,
		admin:        admin,
	}
}

// Run implement the MetricsCollector interface
func (m *MetricsCollector) Run(ctx context.Context) {
}

// Close implement the MetricsCollector interface
func (m *MetricsCollector) Close() {
}

package v2

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
)

type MetricsCollector struct {
	changefeedID model.ChangeFeedID
	role         util.Role
	admin        kafka.ClusterAdminClient
}

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

func (m *MetricsCollector) Run(ctx context.Context) {

}

func (m *MetricsCollector) Close() {

}

package dm

import (
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	jobFactory        = promutil.NewFactory4Framework()
	jobTaskStageGauge = jobFactory.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tiflow",
			Subsystem: "job_master",
			Name:      "job_task_stage",
			Help:      "task stage of running jobs in this cluster",
		}, []string{"job_id"})
)

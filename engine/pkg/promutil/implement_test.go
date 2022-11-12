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

package promutil

import (
	"testing"

	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestWrapCounterOpts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		prefix      string
		constLabels prometheus.Labels
		inputOpts   *prometheus.CounterOpts
		outputOpts  *prometheus.CounterOpts
	}{
		{
			prefix: "",
			inputOpts: &prometheus.CounterOpts{
				Name: "test",
			},
			outputOpts: &prometheus.CounterOpts{
				Name: "test",
			},
		},
		{
			prefix: "DM",
			inputOpts: &prometheus.CounterOpts{
				Namespace: "ns",
				Name:      "test",
			},
			outputOpts: &prometheus.CounterOpts{
				Namespace: "DM_ns",
				Name:      "test",
			},
		},
		{
			constLabels: prometheus.Labels{
				"k2": "v2",
			},
			inputOpts: &prometheus.CounterOpts{
				ConstLabels: prometheus.Labels{
					"k0": "v0",
					"k1": "v1",
				},
			},
			outputOpts: &prometheus.CounterOpts{
				ConstLabels: prometheus.Labels{
					"k0": "v0",
					"k1": "v1",
					"k2": "v2",
				},
			},
		},
		{
			constLabels: prometheus.Labels{
				"k2": "v2",
			},
			inputOpts: &prometheus.CounterOpts{},
			outputOpts: &prometheus.CounterOpts{
				ConstLabels: prometheus.Labels{
					"k2": "v2",
				},
			},
		},
	}

	for _, c := range cases {
		output := wrapCounterOpts(c.prefix, c.constLabels, c.inputOpts)
		require.Equal(t, c.outputOpts, output)
	}
}

func TestWrapCounterOptsLableDuplicate(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		require.NotNil(t, err)
		require.Regexp(t, "duplicate label name", err.(string))
	}()

	constLabels := prometheus.Labels{
		"k0": "v0",
	}
	inputOpts := &prometheus.CounterOpts{
		ConstLabels: prometheus.Labels{
			"k0": "v0",
			"k1": "v1",
		},
	}
	_ = wrapCounterOpts("", constLabels, inputOpts)
	// unreachable
	require.True(t, false)
}

func TestNewCounter(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg)

	tent := tenant.NewProjectInfo(
		"user0",
		"project0",
	)
	tenantID := tent.TenantID()
	projectID := tent.ProjectID()
	labelKey := "k0"
	labelValue := "v0"
	jobType := engineModel.JobTypeDM
	jobID := "job0"
	jobKey := constLabelJobKey
	projectKey := constLabelProjectKey
	tenantKey := constLabelTenantKey

	factory := NewFactory4MasterImpl(
		reg,
		tent,
		jobType.String(),
		jobID,
	)
	counter := factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "syncer",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})
	counter.Inc()
	counter.Add(float64(10))
	var (
		out dto.Metric
		t3  = float64(11)
	)

	require.Nil(t, counter.Write(&out))
	compareMetric(t, &dto.Metric{
		Label: []*dto.LabelPair{
			// all const labels
			{
				Name:  &jobKey,
				Value: &jobID,
			},
			{
				Name:  &labelKey,
				Value: &labelValue,
			},
			{
				Name:  &projectKey,
				Value: &projectID,
			},
			{
				Name:  &tenantKey,
				Value: &tenantID,
			},
		},
		Counter: &dto.Counter{
			Value: &t3,
		},
	},
		&out,
	)

	// different jobID of the same project, but with same metric
	jobID = "job1"
	factory = NewFactory4MasterImpl(
		reg,
		tent,
		jobType.String(),
		jobID,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "syncer",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})

	// different project but with same metric
	tent = tenant.NewProjectInfo(tent.TenantID(), "project1")
	factory = NewFactory4MasterImpl(
		reg,
		tent,
		jobType.String(),
		jobID,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "syncer",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})

	// JobMaster and Worker of the same job type can't has same
	// metric name
	workerID := "worker0"
	factory = NewFactory4WorkerImpl(
		reg,
		tent,
		jobType.String(),
		jobID,
		workerID,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})

	// different workerID of the same job, but with same metric
	workerID = "worker1"
	factory = NewFactory4WorkerImpl(
		reg,
		tent,
		jobType.String(),
		jobID,
		workerID,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})

	// framework with same metric
	factory = NewFactory4FrameworkImpl(
		reg,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})
}

// const label conflict with inner const labels
func TestNewCounterFailConstLabelConflict(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		require.NotNil(t, err)
		require.Regexp(t, "duplicate label name", err.(string))
	}()

	reg := NewRegistry()
	require.NotNil(t, reg)

	factory := NewFactory4MasterImpl(
		reg,
		tenant.NewProjectInfo(
			"user0",
			"proj0",
		),
		engineModel.JobTypeDM.String(),
		"job0",
	)
	_ = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			constLabelJobKey: "job0", // conflict with inner const labels
		},
	})
}

func TestNewCounterVec(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg)

	factory := NewFactory4MasterImpl(
		reg,
		tenant.NewProjectInfo(
			"user0",
			"proj0",
		),
		engineModel.JobTypeDM.String(),
		"job0",
	)
	counterVec := factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			"k1": "v1",
		},
	},
		[]string{"k2", "k3", "k4"},
	)

	counter, err := counterVec.GetMetricWithLabelValues([]string{"v1", "v2", "v3"}...)
	require.NoError(t, err)
	counter.Inc()

	// unmatch label values count
	_, err = counterVec.GetMetricWithLabelValues([]string{"v1", "v2"}...)
	require.Error(t, err)

	counter, err = counterVec.GetMetricWith(prometheus.Labels{
		"k2": "v2", "k3": "v3", "k4": "v4",
	})
	require.NoError(t, err)
	counter.Inc()

	// unmatch label values count
	counter, err = counterVec.GetMetricWith(prometheus.Labels{
		"k3": "v3", "k4": "v4",
	})
	require.Error(t, err)

	require.True(t, counterVec.DeleteLabelValues([]string{"v1", "v2", "v3"}...))
	require.False(t, counterVec.DeleteLabelValues([]string{"v1", "v2"}...))
	require.False(t, counterVec.DeleteLabelValues([]string{"v1", "v2", "v4"}...))

	require.True(t, counterVec.Delete(prometheus.Labels{
		"k2": "v2", "k3": "v3", "k4": "v4",
	}))
	require.False(t, counterVec.Delete(prometheus.Labels{
		"k3": "v3", "k4": "v4",
	}))
	require.False(t, counterVec.Delete(prometheus.Labels{
		"k2": "v3", "k3": "v3", "k4": "v4",
	}))

	curryCounterVec, err := counterVec.CurryWith(prometheus.Labels{
		"k2": "v2",
	})
	require.NoError(t, err)
	counter, err = curryCounterVec.GetMetricWith(prometheus.Labels{
		"k3": "v3", "k4": "v4",
	})
	require.NoError(t, err)
	counter.Add(1)

	// unmatch label values count after curry
	_, err = curryCounterVec.GetMetricWith(prometheus.Labels{
		"k2": "v2", "k3": "v3", "k4": "v4",
	})
	require.Error(t, err)
}

func compareMetric(t *testing.T, expected *dto.Metric, actual *dto.Metric) {
	// compare label pairs
	require.Equal(t, len(expected.Label), len(actual.Label))
	for i, label := range expected.Label {
		require.Equal(t, label.Name, actual.Label[i].Name)
		require.Equal(t, label.Value, actual.Label[i].Value)
	}

	if expected.Counter != nil {
		compareCounter(t, expected.Counter, actual.Counter)
	} else if expected.Gauge != nil {
		compareCounter(t, expected.Counter, actual.Counter)
	} else if expected.Histogram != nil {
		// TODO
	} else {
		require.Fail(t, "unexpected metric type")
	}
}

func compareCounter(t *testing.T, expected *dto.Counter, actual *dto.Counter) {
	require.NotNil(t, expected)
	require.NotNil(t, actual)
	require.Equal(t, expected.Value, actual.Value)
	if expected.Exemplar == nil {
		require.Nil(t, actual.Exemplar)
	} else {
		require.Equal(t, len(expected.Exemplar.Label), len(actual.Exemplar.Label))
		for i, label := range expected.Exemplar.Label {
			require.Equal(t, label.Name, actual.Exemplar.Label[i].Name)
			require.Equal(t, label.Value, actual.Exemplar.Label[i].Value)
		}
	}
}

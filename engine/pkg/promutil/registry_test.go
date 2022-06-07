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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNewRegistry(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg.registry)
	require.Len(t, reg.collectorByWorker, 0)
}

func TestGlobalMetric(t *testing.T) {
	t.Parallel()

	require.Len(t, globalMetricRegistry.collectorByWorker, 1)
	require.Len(t, globalMetricRegistry.collectorByWorker[systemID], 2)
	mfs, err := globalMetricGatherer.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, mfs)
}

func TestMustRegister(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg.registry)

	// normal case, register successfully
	reg.MustRegister("worker0", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter1",
	}))
	reg.MustRegister("worker0", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter2",
	}))
	reg.MustRegister("worker1", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter3",
	}))
	require.Len(t, reg.collectorByWorker, 2)
	require.Len(t, reg.collectorByWorker["worker0"], 2)
	require.Len(t, reg.collectorByWorker["worker1"], 1)

	// metrics name are same, but const labels value are different
	reg.MustRegister("worker2", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter4",
		ConstLabels: prometheus.Labels{
			"k0": "v0",
		},
	}))
	reg.MustRegister("worker2", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter4",
		ConstLabels: prometheus.Labels{
			"k0": "v1",
		},
	}))
	require.Len(t, reg.collectorByWorker["worker2"], 2)

	// metric name are different, but const lables are same
	reg.MustRegister("worker3", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter5",
		ConstLabels: prometheus.Labels{
			"k0": "v0",
		},
	}))
	require.Len(t, reg.collectorByWorker["worker3"], 1)

	// metric name + help + labels name are same,
	reg.MustRegister("worker4", prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "counter6",
		Help: "counter6 help",
		ConstLabels: prometheus.Labels{
			"k0": "v0",
		},
	}, []string{"label0", "label1"}))
	reg.MustRegister("worker4", prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "counter6",
		Help: "counter6 help",
		ConstLabels: prometheus.Labels{
			"k0": "v3",
		},
	}, []string{"label1", "label0"}))
	require.Len(t, reg.collectorByWorker["worker4"], 2)
}

// prometheus metric name and label name only support [a-z][A-Z][0-9][_][:]
// label name can't have prefix '__'
func TestMustRegisterNotValidName(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		require.NotNil(t, err)
		require.Regexp(t, "not a valid metric name", err.(error).Error())
	}()

	reg := NewRegistry()
	require.NotNil(t, reg.registry)

	// not a valid metric name
	reg.MustRegister("worker", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter-5",
	}))
}

func TestMustRegisterNotValidLableName(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		require.NotNil(t, err)
		require.Regexp(t, "not a valid label name", err.(error).Error())
	}()

	reg := NewRegistry()
	require.NotNil(t, reg.registry)

	// not a valid metric name
	reg.MustRegister("worker", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter5",
		ConstLabels: prometheus.Labels{
			"__k0": "v3",
		},
	}))
}

// metric name + const labels are duplicate
func TestMustRegisterFailDuplicateNameLabels(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		require.NotNil(t, err)
		require.Regexp(t, "duplicate metrics collector registration attempted", err.(error).Error())
	}()

	reg := NewRegistry()
	require.NotNil(t, reg.registry)

	reg.MustRegister("worker", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter5",
		ConstLabels: prometheus.Labels{
			"k0": "v0",
		},
	}))
	reg.MustRegister("worker1", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter5",
		ConstLabels: prometheus.Labels{
			"k0": "v0",
		},
	}))
}

// metric names are same, but help + labels name are inconsistent
func TestMustRegisterFailInconsistent(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		require.NotNil(t, err)
		require.Regexp(t, "a previously registered descriptor with the same fully-qualified name", err.(error).Error())
	}()

	reg := NewRegistry()
	require.NotNil(t, reg.registry)

	reg.MustRegister("worker", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter5",
		ConstLabels: prometheus.Labels{
			"k0": "v0",
		},
	}))
	reg.MustRegister("worker1", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter5",
		ConstLabels: prometheus.Labels{
			"k1": "v0",
		},
	}))
}

func TestUnregister(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg.registry)
	reg.MustRegister("worker0", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter5",
		ConstLabels: prometheus.Labels{
			"k0": "v0",
		},
	}))
	reg.MustRegister("worker0", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter5",
		ConstLabels: prometheus.Labels{
			"k0": "v1",
		},
	}))
	reg.MustRegister("worker1", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter6",
		ConstLabels: prometheus.Labels{
			"k0": "v1",
		},
	}))
	require.Len(t, reg.collectorByWorker, 2)
	require.Len(t, reg.collectorByWorker["worker0"], 2)
	require.Len(t, reg.collectorByWorker["worker1"], 1)

	reg.Unregister("worker0")
	require.Len(t, reg.collectorByWorker, 1)
	require.Len(t, reg.collectorByWorker["worker0"], 0)

	// re-register the same metric to check if it's unregister successfully
	reg.MustRegister("worker0", prometheus.NewCounter(prometheus.CounterOpts{
		Name: "counter5",
		ConstLabels: prometheus.Labels{
			"k0": "v0",
		},
	}))
	require.Len(t, reg.collectorByWorker, 2)
}

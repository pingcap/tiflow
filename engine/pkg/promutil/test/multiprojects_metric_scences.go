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

package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		log.L().Info("Start http listen on :8083")
		http.ListenAndServe(":8083", nil)
	}()

	go func() {
		defer wg.Done()
		log.L().Info("Start scenarios simulator")
		simulator(&wg)
	}()

	wg.Wait()
	log.L().Info("Exit scenarios metric test")
}

// simulator will simulate the usage of multi-projects/tasks prometheus metric,
// Includes following scenarios:

/// intra-framework test, SHOULD NOT have same metric name
/// using different service name in K8s to distinguish metric
// 1. one servermaster and one executor
// 2. one servermaster and multi-executors

/// intra-app metric test, SHOULD NOT have same metric
// 3. one jobmaster and one worker of same job type in one executor

/// multi-tasks metric isolation
// 4. multi-jobmasters and multi-workers of same job type in one executor

/// cross app metric isolation
// 5. multi-jobmasters and multi-workers of different job type in one executor

/// app and framework metric isolation
// 6. one jobmaster in one executor, has same original metric name

/// tenant/project metric isolation
// 7. multi-jobmasters of same job type for different project

func simulator(wg *sync.WaitGroup) {
	scenes := []func(wg *sync.WaitGroup){
		scenarios1_OneServerOneExecutor, scenarios2_OneServerMultiExecutor,
		scenarios3_OneJobmasterOneWorker, scenarios4_OneJobmasterMultiWorker,
		scenarios5_MultiJobmasterMultiWorker, scenarios6_OneJobmasterOneExecutor,
		scenarios7_MultiJobmasterMultiProjects,
	}

	for _, scene := range scenes {
		scene(wg)
	}
}

func scenarios1_OneServerOneExecutor(wg *sync.WaitGroup) {
	log.L().Info("Start scenarios1_OneServerOneExecutor simulation...")
	wg.Add(2)
	go func() {
		defer wg.Done()
		// log.L().Info("Start scenarios1_OneServerOneExecutor servermaster")
		// we create new registry here to simulate running an isolation process or container
		reg := promutil.NewRegistry()
		http.Handle("/metric1", promutil.HTTPHandlerForMetricImpl(reg))

		// one server
		factory := promutil.NewFactory4FrameworkImpl(reg)
		counter := factory.NewCounter(prometheus.CounterOpts{
			Namespace: "dfe",
			Subsystem: "servermaster",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr1",
			},
		})
		counter.Add(1)

		for {
			counter.Add(0.1)
			time.Sleep(time.Second)
		}
	}()

	go func() {
		defer wg.Done()
		// log.L().Info("Start scenarios1_OneServerOneExecutor executor")
		// we create new registry here to simulate running an isolation process or container
		reg := promutil.NewRegistry()
		http.Handle("/metric2", promutil.HTTPHandlerForMetricImpl(reg))

		// one server
		factory := promutil.NewFactory4FrameworkImpl(reg)
		counter := factory.NewCounter(prometheus.CounterOpts{
			Namespace: "dfe",
			Subsystem: "executor",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr2",
			},
		})
		counter.Add(2)

		for {
			counter.Add(0.1)
			time.Sleep(time.Second)
		}
	}()
}

func scenarios2_OneServerMultiExecutor(wg *sync.WaitGroup) {
	log.L().Info("Start scenarios2_OneServerMultiExecutor simulation...")
	wg.Add(2)
	// We already create a servermaster in scenarios1_OneServerOneExecutor, so we don't create one here
	go func() {
		defer wg.Done()
		// log.L().Info("Start scenarios2_OneServerMultiExecutor executor0")
		// we create new registry here to simulate running an isolation process or container
		reg := promutil.NewRegistry()
		http.Handle("/metric3", promutil.HTTPHandlerForMetricImpl(reg))

		// one server
		factory := promutil.NewFactory4FrameworkImpl(reg)
		counter := factory.NewCounter(prometheus.CounterOpts{
			Namespace: "dfe",
			Subsystem: "executor",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr3",
			},
		})
		counter.Add(3)

		for {
			counter.Add(0.1)
			time.Sleep(time.Second)
		}
	}()

	go func() {
		defer wg.Done()
		// log.L().Info("Start scenarios2_OneServerMultiExecutor executor1")
		// we create new registry here to simulate running an isolation process or container
		reg := promutil.NewRegistry()
		http.Handle("/metric4", promutil.HTTPHandlerForMetricImpl(reg))

		// one server
		factory := promutil.NewFactory4FrameworkImpl(reg)
		counter := factory.NewCounter(prometheus.CounterOpts{
			Namespace: "dfe",
			Subsystem: "executor",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr4",
			},
		})
		counter.Add(4)

		for {
			counter.Add(0.1)
			time.Sleep(time.Second)
		}
	}()
}

func scenarios3_OneJobmasterOneWorker(wg *sync.WaitGroup) {
	log.L().Info("Start scenarios3_OneJobmasterOneWorker simulation...")
	wg.Add(1)

	tenant := tenant.ProjectInfo{
		TenantID:  "user0",
		ProjectID: "proj0",
	}
	jobType := "DM"
	jobID := "job0"
	workerID := "worker0"

	go func() {
		defer wg.Done()
		// log.L().Info("Start scenarios3_OneJobmasterOneWorker jobmaster")
		// we create new registry here to simulate running an isolation process or container
		reg := promutil.NewRegistry()
		http.Handle("/metric5", promutil.HTTPHandlerForMetricImpl(reg))

		// one jobmaster
		factory := promutil.NewFactory4MasterImpl(reg, tenant, jobType, jobID)
		counter0 := factory.NewCounter(prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "jobmaster",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr5",
			},
		})
		counter0.Add(5)

		// one worker
		// log.L().Info("Start scenarios3_OneJobmasterOneWorker worker0")
		factory = promutil.NewFactory4WorkerImpl(reg, tenant, jobType, jobID, workerID)
		counter1 := factory.NewCounter(prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr5",
			},
		})
		counter1.Add(5)

		counterVec := factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "counter2",

			ConstLabels: prometheus.Labels{
				"service": "svr5",
			},
		},
			[]string{"k1", "k2", "k3"},
		)
		curryCV, err := counterVec.CurryWith(prometheus.Labels{
			"k3": "v3",
		})
		if err != nil {
			log.L().Panic("curry with fail")
		}
		counter2, err := curryCV.GetMetricWithLabelValues([]string{"v1", "v2"}...)
		if err != nil {
			log.L().Panic("GetMetricWithLabelValues")
		}
		counter2.Add(5)

		for {
			counter0.Add(0.1)
			counter1.Add(0.1)
			counter2.Add(0.1)
			time.Sleep(time.Second)
		}
	}()
}

func scenarios4_OneJobmasterMultiWorker(wg *sync.WaitGroup) {
	log.L().Info("Start scenarios4_OneJobmasterMultiWorker simulation...")
	wg.Add(1)

	tenant := tenant.ProjectInfo{
		TenantID:  "user0",
		ProjectID: "proj0",
	}
	jobType := "DM"
	jobID := "job0"

	go func() {
		defer wg.Done()
		// log.L().Info("Start scenarios4_OneJobmasterMultiWorker jobmaster")
		// we create new registry here to simulate running an isolation process or container
		reg := promutil.NewRegistry()
		http.Handle("/metric6", promutil.HTTPHandlerForMetricImpl(reg))

		// one jobmaster
		factory := promutil.NewFactory4MasterImpl(reg, tenant, jobType, jobID)
		counter0 := factory.NewCounter(prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "jobmaster",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr6",
			},
		})
		counter0.Add(6)

		// worker0
		// log.L().Info("Start scenarios4_OneJobmasterMultiWorker worker0")
		factory = promutil.NewFactory4WorkerImpl(reg, tenant, jobType, jobID, "worker0")
		counter1 := factory.NewCounter(prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr6",
			},
		})
		counter1.Add(6)

		// worker1
		// log.L().Info("Start scenarios4_OneJobmasterMultiWorker worker1")
		factory = promutil.NewFactory4WorkerImpl(reg, tenant, jobType, jobID, "worker1")
		counter2 := factory.NewCounter(prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr6",
			},
		})
		counter2.Add(6)

		for {
			counter0.Add(0.1)
			counter1.Add(0.1)
			counter2.Add(0.1)
			time.Sleep(time.Second)
		}
	}()
}

func scenarios5_MultiJobmasterMultiWorker(wg *sync.WaitGroup) {
	log.L().Info("Start scenarios5_MultiJobmasterMultiWorker simulation...")
	wg.Add(1)

	tenant := tenant.ProjectInfo{
		TenantID:  "user0",
		ProjectID: "proj0",
	}
	jobType0 := "DM"
	jobType1 := "CDC"

	jobID0 := "job0"
	jobID1 := "job1"

	workerID0 := "worker0"
	workerID1 := "worker1"

	go func() {
		defer wg.Done()
		// we create new registry here to simulate running an isolation process or container
		reg := promutil.NewRegistry()
		http.Handle("/metric7", promutil.HTTPHandlerForMetricImpl(reg))

		// DM-jobmaster0
		factory := promutil.NewFactory4MasterImpl(reg, tenant, jobType0, jobID0)
		counter0 := factory.NewCounter(prometheus.CounterOpts{
			Subsystem: "jobmaster",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr7",
			},
		})
		counter0.Add(7)

		// DM-jobmaster1
		factory = promutil.NewFactory4MasterImpl(reg, tenant, jobType0, jobID1)
		counter1 := factory.NewCounter(prometheus.CounterOpts{
			Subsystem: "jobmaster",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr7",
			},
		})
		counter1.Add(7)

		// DM-worker0
		factory = promutil.NewFactory4WorkerImpl(reg, tenant, jobType0, jobID0, workerID0)
		counter2 := factory.NewCounter(prometheus.CounterOpts{
			Subsystem: "worker",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr7",
			},
		})
		counter2.Add(7)

		// DM-worker1
		factory = promutil.NewFactory4WorkerImpl(reg, tenant, jobType0, jobID1, workerID1)
		counter3 := factory.NewCounter(prometheus.CounterOpts{
			Subsystem: "worker",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr7",
			},
		})
		counter3.Add(7)

		// CDC-jobmaster0
		factory = promutil.NewFactory4MasterImpl(reg, tenant, jobType1, jobID0)
		counter4 := factory.NewCounter(prometheus.CounterOpts{
			Subsystem: "jobmaster",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr7",
				"type":    "cdc", // same orginal name but const labels are different
			},
		})
		counter4.Add(7)

		// CDC-jobmaster1
		factory = promutil.NewFactory4MasterImpl(reg, tenant, jobType1, jobID1)
		counter5 := factory.NewCounter(prometheus.CounterOpts{
			Subsystem: "jobmaster",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr7",
				"type":    "cdc", // same orginal name but const labels are different
			},
		})
		counter5.Add(7)

		// CDC-worker0
		factory = promutil.NewFactory4WorkerImpl(reg, tenant, jobType1, jobID0, workerID0)
		counter6 := factory.NewCounter(prometheus.CounterOpts{
			Subsystem: "worker",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr7",
				"type":    "cdc", // same orginal name but const labels are different
			},
		})
		counter6.Add(7)

		// CDC-worker1
		factory = promutil.NewFactory4WorkerImpl(reg, tenant, jobType1, jobID1, workerID1)
		counter7 := factory.NewCounter(prometheus.CounterOpts{
			Subsystem: "worker",
			Name:      "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr7",
				"type":    "cdc", // same orginal name but const labels are different
			},
		})
		counter7.Add(7)

		for {
			counter7.Add(0.1)
			counter6.Add(0.1)
			counter5.Add(0.1)
			counter4.Add(0.1)
			counter3.Add(0.1)
			counter2.Add(0.1)
			counter1.Add(0.1)
			counter0.Add(0.1)
			time.Sleep(time.Second)
		}
	}()
}

func scenarios6_OneJobmasterOneExecutor(wg *sync.WaitGroup) {
	log.L().Info("Start scenarios6_OneJobmasterOneExecutor simulation...")
	wg.Add(1)

	tenant := tenant.ProjectInfo{
		TenantID:  "user0",
		ProjectID: "proj0",
	}
	jobType := "DM"
	jobID := "job0"

	go func() {
		defer wg.Done()
		// we create new registry here to simulate running an isolation process or container
		reg := promutil.NewRegistry()
		http.Handle("/metric8", promutil.HTTPHandlerForMetricImpl(reg))

		// one jobmaster
		factory := promutil.NewFactory4MasterImpl(reg, tenant, jobType, jobID)
		counter0 := factory.NewCounter(prometheus.CounterOpts{
			Name: "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr8",
			},
		})
		counter0.Add(8)

		// one worker
		factory = promutil.NewFactory4FrameworkImpl(reg)
		counter1 := factory.NewCounter(prometheus.CounterOpts{
			Name: "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr8",
			},
		})
		counter1.Add(8)

		for {
			counter0.Add(0.1)
			counter1.Add(0.1)
			time.Sleep(time.Second)
		}
	}()
}

func scenarios7_MultiJobmasterMultiProjects(wg *sync.WaitGroup) {
	log.L().Info("Start scenarios7_MultiJobmasterMultiProjects simulation...")
	wg.Add(1)

	tenant0 := tenant.ProjectInfo{
		TenantID:  "user0",
		ProjectID: "proj0",
	}
	tenant1 := tenant.ProjectInfo{
		TenantID:  "user1",
		ProjectID: "proj1",
	}
	jobType := "DM"
	jobID := "job0"

	go func() {
		defer wg.Done()
		// we create new registry here to simulate running an isolation process or container
		reg := promutil.NewRegistry()
		http.Handle("/metric9", promutil.HTTPHandlerForMetricImpl(reg))

		// project0
		factory := promutil.NewFactory4MasterImpl(reg, tenant0, jobType, jobID)
		counter0 := factory.NewCounter(prometheus.CounterOpts{
			Name: "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr9",
			},
		})
		counter0.Add(9)

		// project1
		factory = promutil.NewFactory4MasterImpl(reg, tenant1, jobType, jobID)
		counter1 := factory.NewCounter(prometheus.CounterOpts{
			Name: "counter",

			ConstLabels: prometheus.Labels{
				"service": "svr9",
			},
		})
		counter1.Add(9)

		for {
			counter0.Add(0.1)
			counter1.Add(0.1)
			time.Sleep(time.Second)
		}
	}()
}

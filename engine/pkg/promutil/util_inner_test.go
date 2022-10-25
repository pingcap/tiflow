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

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNewFactory4JobMaster(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg)

	cases := []struct {
		info    tenant.ProjectInfo
		jobType engineModel.JobType
		jobID   engineModel.JobID
		output  Factory
	}{
		{
			info: tenant.NewProjectInfo(
				"user0",
				"project0",
			),
			jobType: engineModel.JobTypeDM,
			jobID:   "job0",
			output: &AutoRegisterFactory{
				inner: &WrappingFactory{
					inner:  &PromFactory{},
					prefix: "DM",
					constLabels: prometheus.Labels{
						constLabelTenantKey:  "user0",
						constLabelProjectKey: "project0",
						constLabelJobKey:     "job0",
					},
				},
				r:  reg,
				id: "job0",
			},
		},
	}

	for _, c := range cases {
		f := NewFactory4MasterImpl(reg, c.info, c.jobType.String(), c.jobID)
		require.Equal(t, c.output, f)
	}
}

func TestNewFactory4Worker(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg)
	cases := []struct {
		info     tenant.ProjectInfo
		jobType  engineModel.JobType
		jobID    frameModel.MasterID
		workerID frameModel.WorkerID
		output   Factory
	}{
		{
			info: tenant.NewProjectInfo(
				"user0",
				"project0",
			),
			jobType:  engineModel.JobTypeDM,
			jobID:    "job0",
			workerID: "worker0",
			output: &AutoRegisterFactory{
				inner: &WrappingFactory{
					inner:  &PromFactory{},
					prefix: "DM",
					constLabels: prometheus.Labels{
						constLabelTenantKey:  "user0",
						constLabelProjectKey: "project0",
						constLabelJobKey:     "job0",
						constLabelWorkerKey:  "worker0",
					},
				},
				r:  reg,
				id: "worker0",
			},
		},
	}

	for _, c := range cases {
		f := NewFactory4WorkerImpl(reg, c.info, c.jobType.String(), c.jobID, c.workerID)
		require.Equal(t, c.output, f)
	}
}

func TestNewFactory4Framework(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg)
	cases := []struct {
		output Factory
	}{
		{
			output: &AutoRegisterFactory{
				inner: &WrappingFactory{
					inner:  &PromFactory{},
					prefix: frameworkMetricPrefix,
					constLabels: prometheus.Labels{
						constLabelFrameworkKey: "true",
					},
				},
				r:  reg,
				id: frameworkID,
			},
		},
	}

	for _, c := range cases {
		f := NewFactory4FrameworkImpl(reg)
		require.Equal(t, c.output, f)
	}
}

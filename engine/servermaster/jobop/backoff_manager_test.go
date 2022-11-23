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

package jobop

import (
	"testing"

	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/stretchr/testify/require"
)

func TestJobBackoffManager(t *testing.T) {
	t.Parallel()

	jobID := "mgr-test-job"
	clocker := clock.NewMock()
	mgr := NewBackoffManagerImpl(clocker, NewDefaultBackoffConfig())

	require.True(t, mgr.Allow(jobID))
	mgr.JobOnline(jobID)

	require.True(t, mgr.Allow(jobID))
	mgr.JobFail(jobID)
	require.False(t, mgr.Allow(jobID))

	clocker.Add(defaultBackoffInitInterval * 2)
	require.True(t, mgr.Allow(jobID))

	mgr.JobTerminate(jobID)
	require.NotContains(t, mgr.jobs, jobID)
}

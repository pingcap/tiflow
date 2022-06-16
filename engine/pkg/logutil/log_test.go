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

package logutil

import (
	"regexp"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestNewLogger(t *testing.T) {
	var buffer zaptest.Buffer
	lg, property, err := log.InitLoggerWithWriteSyncer(&log.Config{
		Level:  "warn",
		Format: "text",
	}, &buffer, nil)
	require.NoError(t, err)
	log.ReplaceGlobals(lg, property)

	logger := NewLogger4Framework()
	logger.Warn("framework test", zap.String("type", "framework"))
	require.Regexp(t, regexp.QuoteMeta("[\"framework test\"] [framework=true] [type=framework]"), buffer.Stripped())

	logger = NewLogger4Master(tenant.NewProjectInfo("tenant1", "proj1"), "job1")
	logger.Warn("master test", zap.String("type", "master"))
	require.Regexp(t, regexp.QuoteMeta("[\"master test\"] [tenant=tenant1] [project_id=proj1] [job_id=job1] [type=master]"), buffer.Stripped())

	logger = NewLogger4Worker(tenant.NewProjectInfo("tenant1", "proj1"), "job1", "worker1")
	logger.Warn("worker test", zap.String("type", "worker"))
	require.Regexp(t, regexp.QuoteMeta("[\"worker test\"] [tenant=tenant1] [project_id=proj1] [job_id=job1] [worker_id=worker1] [type=worker]"), buffer.Stripped())
}

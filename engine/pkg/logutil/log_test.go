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
	"os"
	"regexp"
	"testing"

	"github.com/hpcloud/tail"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var testFile = "test.log"

func init() {
	logger, prop, err := log.InitLogger(&log.Config{
		Level: "warn",
		File: log.FileLogConfig{
			Filename: testFile,
		},
	})
	if err != nil {
		panic(err)
	}
	log.ReplaceGlobals(logger, prop)
}

func TestNewLogger(t *testing.T) {
	defer os.Remove(testFile)
	tail, err := tail.TailFile(testFile, tail.Config{Follow: true})
	require.NoError(t, err)
	defer tail.Stop()

	logger := NewLogger4Framework()
	logger.Warn("framework test", zap.String("type", "framework"))
	logger.Sync()
	line := <-tail.Lines
	require.Regexp(t, regexp.QuoteMeta("[\"framework test\"] [framework=true] [type=framework]"), line.Text)

	logger = NewLogger4Master(tenant.NewProjectInfo("tenant1", "proj1"), "job1")
	logger.Warn("master test", zap.String("type", "master"))
	logger.Sync()
	line = <-tail.Lines
	require.Regexp(t, regexp.QuoteMeta("[\"master test\"] [tenant=tenant1] [project_id=proj1] [job_id=job1] [type=master]"), line.Text)

	logger = NewLogger4Worker(tenant.NewProjectInfo("tenant1", "proj1"), "job1", "worker1")
	logger.Warn("worker test", zap.String("type", "worker"))
	logger.Sync()
	line = <-tail.Lines
	require.Regexp(t, regexp.QuoteMeta("[\"worker test\"] [tenant=tenant1] [project_id=proj1] [job_id=job1] [worker_id=worker1] [type=worker]"), line.Text)
}

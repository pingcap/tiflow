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
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/hpcloud/tail"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewLogger(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test.log")
	os.Remove(testFile)
	lg, prop, err := log.InitLogger(&log.Config{
		Level: "warn",
		File: log.FileLogConfig{
			Filename: testFile,
		},
	})
	require.NoError(t, err)
	log.ReplaceGlobals(lg, prop)

	defer os.Remove(testFile)
	var wg sync.WaitGroup
	defer wg.Wait()

	tailHandler, err := tail.TailFile(testFile, tail.Config{Follow: true, Poll: true})
	require.NoError(t, err)
	defer tailHandler.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	lineCh := make(chan string, 10)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(lineCh)
		for {
			select {
			case line, ok := <-tailHandler.Lines:
				if !ok {
					return
				}
				require.NoError(t, line.Err)
				lineCh <- line.Text
			case <-ctx.Done():
				return
			}
		}
	}()

	logger := NewLogger4Framework()
	logger.Warn("framework test", zap.String("type", "framework"))
	logger.Sync()
	line := <-lineCh
	require.Regexp(t, regexp.QuoteMeta("[\"framework test\"] [framework=true] [type=framework]"), line)

	logger = NewLogger4Master(tenant.NewProjectInfo("tenant1", "proj1"), "job1")
	logger.Warn("master test", zap.String("type", "master"))
	logger.Sync()
	line = <-lineCh
	require.Regexp(t, regexp.QuoteMeta("[\"master test\"] [tenant=tenant1] [project_id=proj1] [job_id=job1] [type=master]"), line)

	logger = NewLogger4Worker(tenant.NewProjectInfo("tenant1", "proj1"), "job1", "worker1")
	logger.Warn("worker test", zap.String("type", "worker"))
	logger.Sync()
	line = <-lineCh
	require.Regexp(t, regexp.QuoteMeta("[\"worker test\"] [tenant=tenant1] [project_id=proj1] [job_id=job1] [worker_id=worker1] [type=worker]"), line)
}

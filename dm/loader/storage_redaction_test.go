// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loader

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func TestCleanRemoteDumpFiles(t *testing.T) {
	file := filepath.Join(t.TempDir(), "cleanup.log")
	require.NoError(t, log.InitLogger(&log.Config{Level: "info", File: file}))
	t.Cleanup(func() {
		_ = log.L().Sync()
		_ = log.InitLogger(&log.Config{Level: "info"})
	})
	for _, dir := range []string{
		"s3://bucket/path?secret-access-key=private-sk",
		"gs://bucket/path",
		"azure://container/path?account-key=private-key",
	} {
		cfg := &config.SubTaskConfig{Mode: config.ModeAll, LoaderConfig: config.LoaderConfig{Dir: dir}}
		cleanDumpFiles(context.Background(), cfg)
		require.Equal(t, dir, cfg.Dir)
	}
	require.NoError(t, log.L().Sync())
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	require.NotContains(t, string(data), "private-")
	require.NotContains(t, string(data), "fail to collect files")
	require.Contains(t, string(data), "remote storage does not need")
}

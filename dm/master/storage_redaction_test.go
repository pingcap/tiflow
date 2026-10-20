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

package master

import (
	"testing"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/stretchr/testify/require"
)

func TestTaskConfigStorageResponse(t *testing.T) {
	cfg := config.NewTaskConfig()
	cfg.Name = "task-one"
	cfg.Loaders["local"] = &config.LoaderConfig{Dir: "./dump"}
	require.Equal(t, cfg.String(), taskConfigWithRedactedStorage(cfg).String(),
		"get-config formatting must not change for ordinary configurations")
	raw := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk"
	cfg.Loaders["remote"] = &config.LoaderConfig{Dir: raw}
	cfg.MySQLInstances = []*config.MySQLInstance{{Loader: cfg.Loaders["remote"]}}
	before := cfg.String()
	output := taskConfigWithRedactedStorage(cfg).String()
	require.NotContains(t, output, "private-")
	require.Contains(t, output, "bucket/path")
	require.Equal(t, before, cfg.String())
	require.Equal(t, raw, cfg.MySQLInstances[0].Loader.Dir)
}

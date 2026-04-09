// Copyright 2025 PingCAP, Inc.
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

package syncer

import (
	"testing"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func TestNewDDLRewriterHonorsMariaDBCompatMode(t *testing.T) {
	logger := log.L()

	cfg := &config.SubTaskConfig{
		Flavor:        "mariadb",
		MariaDBCompat: config.DefaultMariaDBCompatConfig(),
	}
	require.NotNil(t, newDDLRewriter(cfg, logger))

	cfg.MariaDBCompat.Mode = config.MariaDBCompatModeOff
	require.NoError(t, cfg.MariaDBCompat.Adjust())
	require.Nil(t, newDDLRewriter(cfg, logger))

	cfg.Flavor = "mysql"
	cfg.MariaDBCompat.Mode = config.MariaDBCompatModeOn
	require.NoError(t, cfg.MariaDBCompat.Adjust())
	require.NotNil(t, newDDLRewriter(cfg, logger))

	cfg.MariaDBCompat.Mode = config.MariaDBCompatModeAuto
	require.NoError(t, cfg.MariaDBCompat.Adjust())
	require.Nil(t, newDDLRewriter(cfg, logger))
}

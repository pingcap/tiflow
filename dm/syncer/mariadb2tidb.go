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
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	mariadb2tidb "github.com/pingcap/tiflow/dm/pkg/mariadb2tidb"
	mconfig "github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/config"
	"go.uber.org/zap"
)

func newDDLConverter(cfg *config.SubTaskConfig, logger log.Logger) *mariadb2tidb.Converter {
	if cfg == nil || !cfg.MariaDB2TiDB.EnabledForFlavor(cfg.Flavor) {
		return nil
	}

	convCfg := mconfig.DefaultConfig()
	convCfg.EnabledRules = append([]string{}, cfg.MariaDB2TiDB.EnabledRules...)
	convCfg.DisabledRules = append([]string{}, cfg.MariaDB2TiDB.DisabledRules...)
	convCfg.StrictMode = cfg.MariaDB2TiDB.Strict()

	logger.Info("enable mariadb2tidb DDL conversion",
		zap.String("source", cfg.SourceID),
		zap.String("mode", cfg.MariaDB2TiDB.Mode),
		zap.Bool("strict", cfg.MariaDB2TiDB.Strict()),
	)
	return mariadb2tidb.NewConverter(convCfg)
}

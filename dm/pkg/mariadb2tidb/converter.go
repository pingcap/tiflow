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

package mariadb2tidb

import (
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/config"
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/parser"
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/transformer"
)

// Converter transforms MariaDB schema SQL into TiDB-compatible SQL.
// The rules and preprocessing logic are adapted from the mariadb2tidb project.
type Converter struct {
	cfg    *config.Config
	engine *transformer.Engine
}

// NewConverter creates a Converter with the provided config.
// If cfg is nil, DefaultConfig is used.
func NewConverter(cfg *config.Config) *Converter {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	return &Converter{
		cfg:    cfg,
		engine: transformer.NewEngine(cfg),
	}
}

// TransformSQL applies preprocessing and rules to a SQL string.
// When strict mode is disabled, it returns the original SQL on error.
func (c *Converter) TransformSQL(sql string) (string, error) {
	loader := parser.NewLoaderWithConfig(c.cfg)
	stmts, err := loader.LoadFromString(sql)
	if err != nil {
		if !c.cfg.StrictMode {
			return sql, nil
		}
		return "", err
	}
	if len(stmts) == 0 {
		return "", nil
	}

	transformed, err := c.engine.Transform(stmts)
	if err != nil {
		if !c.cfg.StrictMode {
			return sql, nil
		}
		return "", err
	}

	writer := parser.NewWriter()
	output, err := writer.WriteStatements(transformed)
	if err != nil {
		if !c.cfg.StrictMode {
			return sql, nil
		}
		return "", err
	}
	return output, nil
}

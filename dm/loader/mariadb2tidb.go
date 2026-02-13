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

package loader

import (
	"context"
	"sort"
	"strings"

	extstorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb"
	mconfig "github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/config"
	"github.com/pingcap/tiflow/dm/pkg/storage"
	"go.uber.org/zap"
)

func (l *LightningLoader) maybeTransformSchemaFiles(ctx context.Context) error {
	if l.cfg == nil || !l.cfg.MariaDB2TiDB.EnabledForFlavor(l.cfg.Flavor) {
		return nil
	}

	dumpStorage, closeFn, err := l.dumpStorage(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	files, err := storage.CollectDirFiles(ctx, l.cfg.LoaderConfig.Dir, dumpStorage)
	if err != nil {
		return err
	}

	schemaFiles := make([]string, 0, len(files))
	for name := range files {
		if isSchemaFile(name) {
			schemaFiles = append(schemaFiles, name)
		}
	}
	if len(schemaFiles) == 0 {
		return nil
	}
	sort.Strings(schemaFiles)

	convCfg := mconfig.DefaultConfig()
	convCfg.EnabledRules = append([]string{}, l.cfg.MariaDB2TiDB.EnabledRules...)
	convCfg.DisabledRules = append([]string{}, l.cfg.MariaDB2TiDB.DisabledRules...)
	convCfg.StrictMode = l.cfg.MariaDB2TiDB.Strict()
	converter := mariadb2tidb.NewConverter(convCfg)

	l.logger.Info("transforming MariaDB schema files for TiDB compatibility",
		zap.String("dir", l.cfg.LoaderConfig.Dir),
		zap.Int("files", len(schemaFiles)),
	)

	for _, name := range schemaFiles {
		raw, err := dumpStorage.ReadFile(ctx, name)
		if err != nil {
			return err
		}

		transformed, err := converter.TransformSQL(string(raw))
		if err != nil {
			return err
		}
		if strings.TrimSpace(transformed) == "" || transformed == string(raw) {
			continue
		}

		if err := dumpStorage.WriteFile(ctx, name, []byte(transformed)); err != nil {
			return err
		}
	}
	return nil
}

func (l *LightningLoader) dumpStorage(ctx context.Context) (extstorage.ExternalStorage, func(), error) {
	if l.cfg.ExtStorage != nil {
		return l.cfg.ExtStorage, func() {}, nil
	}
	created, err := storage.CreateStorage(ctx, l.cfg.LoaderConfig.Dir)
	if err != nil {
		return nil, nil, err
	}
	return created, created.Close, nil
}

func isSchemaFile(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".sql") && strings.Contains(lower, "-schema")
}

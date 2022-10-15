// Copyright 2020 PingCAP, Inc.
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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/diff"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// diffDataLoop checks whether target has the same data with source via `sync-diff-inspector` multiple times.
func diffDataLoop(ctx context.Context, count int, interval time.Duration, schema string, tables []string, targetDB *sql.DB, sourceDBs ...*sql.DB) (err error) {
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
			err = diffData(ctx, schema, tables, targetDB, sourceDBs...)
			if err == nil {
				return nil
			}
			log.L().Warn("diff data error", zap.Int("count", i+1), log.ShortError(err))
		}
	}
	return err
}

// diffData checks whether target has the same data with source via `sync-diff-inspector`.
func diffData(ctx context.Context, schema string, tables []string, targetDB *sql.DB, sourceDBs ...*sql.DB) error {
	for _, table := range tables {
		sourceTables := make([]*diff.TableInstance, 0, len(sourceDBs))
		for i, sourceDB := range sourceDBs {
			sourceTables = append(sourceTables, &diff.TableInstance{
				Conn:       sourceDB,
				Schema:     schema,
				Table:      table,
				InstanceID: fmt.Sprintf("source-%d", i),
			})
		}

		targetTable := &diff.TableInstance{
			Conn:       targetDB,
			Schema:     schema,
			Table:      table,
			InstanceID: "target",
		}

		td := &diff.TableDiff{
			SourceTables:     sourceTables,
			TargetTable:      targetTable,
			ChunkSize:        1000,
			Sample:           100,
			CheckThreadCount: 1,
			UseChecksum:      true,
			TiDBStatsSource:  targetTable,
			CpDB:             targetDB,
		}

		structEqual, dataEqual, err := td.Equal(ctx, func(dml string) error {
			return nil
		})

		if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded {
			return nil
		}
		if !structEqual {
			return fmt.Errorf("different struct for table %s", dbutil.TableName(schema, table))
		} else if !dataEqual {
			return fmt.Errorf("different data for table %s", dbutil.TableName(schema, table))
		}
		log.L().Info("data equal for table", zap.String("schema", schema), zap.String("table", table))
	}

	return nil
}

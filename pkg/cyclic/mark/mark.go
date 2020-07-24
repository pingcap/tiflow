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

package mark

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/quotes"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
)

const (
	// SchemaName is the name of schema where all mark tables are created
	SchemaName string = "tidb_cdc"
	tableName  string = "repl_mark"

	// CyclicReplicaIDCol is the name of replica ID in mark tables
	CyclicReplicaIDCol string = "replica_id"

	// OptCyclicConfig is the key that adds to changefeed options
	// automatically is cyclic replication is on.
	OptCyclicConfig string = "_cyclic_relax_sql_mode"
)

// GetMarkTableName returns mark table name regards to the tableID
func GetMarkTableName(sourceSchema, sourceTable string) (schema, table string) { // nolint:exported
	// TODO(neil) better unquote or just crc32 the name.
	table = strings.Join([]string{tableName, sourceSchema, sourceTable}, "_")
	schema = SchemaName
	return
}

// IsMarkTable tells whether the table is a mark table or not.
func IsMarkTable(schema, table string) bool {
	const quoteSchemaName = "`" + SchemaName + "`"
	const quotetableName = "`" + tableName

	if schema == SchemaName || schema == quoteSchemaName {
		return true
	}
	if strings.HasPrefix(table, quotetableName) {
		return true
	}
	return strings.HasPrefix(table, tableName)
}

// TableName is an interface gets schema and table name.
// Note it is only used for avoiding import model.TableName.
type TableName interface {
	GetSchema() string
	GetTable() string
}

// CreateMarkTables creates mark table regard to the table name.
//
// Note table name is only for avoid write hotspot there is *NO* guarantee
// normal tables and mark tables are one:one map.
func CreateMarkTables(ctx context.Context, upstreamDSN string, upstreamCred *security.Credential, tables ...TableName) error {
	tlsCfg, err := upstreamCred.ToTLSConfig()
	if err != nil {
		return errors.Annotate(err, "fail to open upstream TiDB connection")
	}
	if tlsCfg != nil {
		tlsName := "cli-marktable"
		err = mysql.RegisterTLSConfig(tlsName, tlsCfg)
		if err != nil {
			return errors.Annotate(err, "fail to open upstream TiDB connection")
		}
		if strings.Contains(upstreamDSN, "?") && strings.Contains(upstreamDSN, "=") {
			upstreamDSN += ("&tls=" + tlsName)
		} else {
			upstreamDSN += ("?tls=" + tlsName)
		}
	}
	db, err := sql.Open("mysql", upstreamDSN)
	if err != nil {
		return errors.Annotate(err, "Open upsteam database connection failed")
	}
	err = db.PingContext(ctx)
	if err != nil {
		return errors.Annotate(err, "fail to open upstream TiDB connection")
	}

	userTableCount := 0
	for _, name := range tables {
		if IsMarkTable(name.GetSchema(), name.GetTable()) {
			continue
		}
		userTableCount++
		schema, table := GetMarkTableName(name.GetSchema(), name.GetTable())
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", schema))
		if err != nil {
			return errors.Annotate(err, "fail to create mark database")
		}
		_, err = db.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s
			(
				bucket INT NOT NULL,
				%s BIGINT UNSIGNED NOT NULL,
				val BIGINT DEFAULT 0,
				start_timestamp BIGINT DEFAULT 0,
				PRIMARY KEY (bucket, %s)
			);`, quotes.QuoteSchema(schema, table), CyclicReplicaIDCol, CyclicReplicaIDCol))
		if err != nil {
			return errors.Annotatef(err, "fail to create mark table %s", table)
		}
	}
	log.Info("create upstream mark done", zap.Int("count", userTableCount))
	return nil
}

// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"
	"database/sql"
	"strings"

	"github.com/pingcap/parser/model"

	"github.com/cenkalti/backoff"

	_ "github.com/pingcap/tidb/types/parser_driver"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type mysqlSink struct {
	db *sql.DB
}

var _ Sink = &mysqlSink{}

func (s *mysqlSink) Emit(ctx context.Context, txn Txn) error {
	if txn.IsDDL() {
		return s.execDDLWithMaxRetries(ctx, txn.DDL, 5)
	}
	// TODO: Handle DML
	return nil
}

func (s *mysqlSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved uint64) error {
	return nil
}

func (s *mysqlSink) Flush(ctx context.Context) error {
	return nil
}

func (s *mysqlSink) Close() error {
	return nil
}

func (s *mysqlSink) execDDLWithMaxRetries(ctx context.Context, ddl *DDL, maxRetries uint64) error {
	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		maxRetries,
	)
	return backoff.Retry(func() error {
		// TODO: Wrap context canceled or deadline exceeded as permanent errors
		return s.execDDL(ctx, ddl)
	}, retryCfg)
}

func (s *mysqlSink) execDDL(ctx context.Context, ddl *DDL) error {
	shouldSwitchDB := len(ddl.Database) > 0 && ddl.Type != model.ActionCreateSchema

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+quoteName(ddl.Database)+";")
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	if _, err = tx.ExecContext(ctx, ddl.SQL); err != nil {
		tx.Rollback()
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	log.Info("Exec DDL succeeded", zap.String("sql", ddl.SQL))
	return nil
}

func quoteName(name string) string {
	return "`" + escapeName(name) + "`"
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

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
	"math/rand"
	"strings"
	"time"

	"github.com/chaos-mesh/go-sqlsmith"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	config2 "github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	tableCount      = 10               // tables count in schema.
	fullInsertCount = 100              // `INSERT INTO` count (not rows count) for each table in full stage.
	diffCount       = 20               // diff data check count
	diffInterval    = 20 * time.Second // diff data check interval
	incrRoundTime   = 10 * time.Second // time to generate incremental data in one round
)

// task is a data migration task test case with one or more sources.
type task struct {
	logger log.Logger
	ctx    context.Context

	cli pb.MasterClient
	ss  []*sqlsmith.SQLSmith

	sourceDBs   []*conn.BaseDB
	sourceConns []*dbConn
	targetDB    *conn.BaseDB
	targetConn  *dbConn

	schema  string
	tables  []string
	taskCfg config2.TaskConfig
	results results

	caseGenerator *CaseGenerator
}

// newTask creates a new task instance.
func newTask(ctx context.Context, cli pb.MasterClient, taskFile string, schema string,
	targetCfg dbconfig.DBConfig, sourcesCfg ...dbconfig.DBConfig,
) (*task, error) {
	var taskCfg config2.TaskConfig
	err := taskCfg.DecodeFile(taskFile)
	if err != nil {
		return nil, err
	}
	taskCfg.TargetDB = &targetCfg // replace DB config

	var (
		sourceDBs   = make([]*conn.BaseDB, 0, len(taskCfg.MySQLInstances))
		sourceConns = make([]*dbConn, 0, len(taskCfg.MySQLInstances))
		res         = make(results, 0, len(taskCfg.MySQLInstances))
	)
	for i, m := range taskCfg.MySQLInstances { // only use necessary part of sources.
		// reset Syncer, otherwise will report ERROR 20017
		if len(m.SyncerConfigName) > 0 && m.Syncer != nil {
			m.Syncer = nil
		}

		cfg := sourcesCfg[i]
		db, err2 := conn.GetUpstreamDB(&cfg)
		if err2 != nil {
			return nil, err2
		}
		dbConnection, err2 := createDBConn(ctx, db, schema)
		if err2 != nil {
			return nil, err2
		}
		if taskCfg.CaseSensitive {
			lcSetting, err2 := conn.FetchLowerCaseTableNamesSetting(ctx, dbConnection.baseConn)
			if err2 != nil {
				return nil, err2
			}
			if lcSetting == conn.LCTableNamesMixed {
				msg := "can not set `case-sensitive = true` when upstream `lower_case_table_names = 2`"
				log.L().Error(msg, zap.Any("instance", cfg))
				return nil, errors.New(msg)
			}
		}
		sourceDBs = append(sourceDBs, db)
		sourceConns = append(sourceConns, dbConnection)
		res = append(res, singleResult{})
	}

	targetDB, err := conn.GetDownstreamDB(&targetCfg)
	if err != nil {
		return nil, err
	}
	targetConn, err := createDBConn(ctx, targetDB, schema)
	if err != nil {
		return nil, err
	}

	t := &task{
		logger:        log.L().WithFields(zap.String("case", taskCfg.Name)),
		ctx:           ctx,
		cli:           cli,
		ss:            make([]*sqlsmith.SQLSmith, len(taskCfg.MySQLInstances)),
		sourceDBs:     sourceDBs,
		sourceConns:   sourceConns,
		targetDB:      targetDB,
		targetConn:    targetConn,
		schema:        schema,
		tables:        make([]string, 0),
		taskCfg:       taskCfg,
		results:       res,
		caseGenerator: NewCaseGenerator(taskCfg.ShardMode),
	}
	for i := 0; i < len(t.ss); i++ {
		t.ss[i] = sqlsmith.New()
		t.ss[i].SetDB(schema)
	}
	return t, nil
}

// run runs the case.
func (t *task) run() error {
	defer func() {
		for _, db := range t.sourceDBs {
			db.Close()
		}
		t.targetDB.Close()

		t.logger.Info("task runs results", zap.Stringer("results", t.results))
	}()

	if err := t.stopPreviousTask(); err != nil {
		return err
	}
	if err := t.clearPreviousData(); err != nil {
		return err
	}

	if err := t.genFullData(); err != nil {
		return err
	}

	if err := t.createTask(); err != nil {
		return err
	}

	t.logger.Info("check data for full stage")
	sourceDBs := make([]*sql.DB, 0, len(t.sourceDBs))
	for _, db := range t.sourceDBs {
		sourceDBs = append(sourceDBs, db.DB)
	}
	if err := diffDataLoop(t.ctx, diffCount, diffInterval, t.schema, t.tables, t.targetDB.DB, sourceDBs...); err != nil {
		return err
	}

	return t.incrLoop()
}

// stopPreviousTask stops the previous task with the same name if exists.
func (t *task) stopPreviousTask() error {
	t.logger.Info("stopping previous task")
	resp, err := t.cli.OperateTask(t.ctx, &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Delete,
		Name: t.taskCfg.Name,
	})
	if err != nil {
		return err
	} else if !resp.Result && !strings.Contains(resp.Msg, "not exist") {
		return fmt.Errorf("fail to stop task: %s", resp.Msg)
	}
	return nil
}

// clearPreviousData clears previous data in upstream source and downstream target.
func (t *task) clearPreviousData() error {
	t.logger.Info("clearing previous source and target data")
	for _, conn := range t.sourceConns {
		if err := dropDatabase(t.ctx, conn, t.schema); err != nil {
			return err
		}
	}
	return dropDatabase(t.ctx, t.targetConn, t.schema)
}

// genFullData generates data for the full stage.
func (t *task) genFullData() error {
	t.logger.Info("generating data for full stage")
	for _, conn := range t.sourceConns {
		if err := createDatabase(t.ctx, conn, t.schema); err != nil {
			return err
		}
		// NOTE: we set CURRENT database here.
		if err := conn.execSQLs(t.ctx, fmt.Sprintf("USE %s", t.schema)); err != nil {
			return err
		}
	}

	var (
		columns = make([][5]string, 0)
		indexes = make(map[string][]string)
	)

	// generate `CREATE TABLE` statements.
	for i := 0; i < tableCount; i++ {
		query, name, err := t.ss[0].CreateTableStmt()
		if err != nil {
			return err
		}
		t.logger.Info("creating table", zap.String("query", query))
		for j, conn := range t.sourceConns {
			if err = conn.execSQLs(t.ctx, query); err != nil {
				return err
			}
			// set different `AUTO_INCREMENT` to avoid encplicate entry for `INSERT`.
			if err = conn.execSQLs(t.ctx, fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = %d", name, 1+j*100000000)); err != nil {
				return err
			}
		}
		t.tables = append(t.tables, name)

		col2, idx2, err := createTableToSmithSchema(t.schema, query)
		if err != nil {
			return err
		}
		columns = append(columns, col2...)
		indexes[name] = idx2
	}

	for i := 0; i < len(t.ss); i++ {
		// go-sqlsmith needs to load schema before generating DML and `ALTER TABLE` statements.
		t.ss[i].LoadSchema(columns, indexes)
	}

	var eg errgroup.Group
	for _, conn := range t.sourceConns {
		conn2 := conn
		eg.Go(func() error {
			for i := 0; i < fullInsertCount; i++ {
				query, _, err2 := t.ss[0].InsertStmt(false)
				if err2 != nil {
					return err2
				}
				if err2 = conn2.execSQLs(t.ctx, query); err2 != nil {
					return err2
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// createTask does `start-task` operation.
func (t *task) createTask() error {
	t.logger.Info("starting the task", zap.String("task cfg", t.taskCfg.String()))
	resp, err := t.cli.StartTask(t.ctx, &pb.StartTaskRequest{
		Task: t.taskCfg.String(),
	})
	if err != nil {
		return err
	} else if !resp.Result && !strings.Contains(resp.Msg, "already exist") { // imprecise match
		return fmt.Errorf("fail to start task: %s", resp.Msg)
	}
	return nil
}

// incrLoop enters the loop of generating incremental data and diff them.
func (t *task) incrLoop() error {
	t.caseGenerator.Start(t.ctx, t.schema, t.tables)

	// execute preSQLs in upstream
	for _, sql := range t.caseGenerator.GetPreSQLs() {
		if err := t.sourceConns[sql.source].execDDLs(t.ctx, sql.statement); err != nil {
			return err
		}
	}
	if err := t.updateSchema(); err != nil {
		return err
	}

	for {
		select {
		case <-t.ctx.Done():
			return nil
		default:
			ctx2, cancel2 := context.WithTimeout(t.ctx, incrRoundTime)
			// generate data
			err := t.genIncrData(ctx2)
			if err != nil {
				cancel2()
				return err
			}

			// diff data
			err = t.diffIncrData(t.ctx)
			if err != nil {
				cancel2()
				return err
			}
			cancel2()
		}
	}
}

// genIncrData generates data for the incremental stage in one round.
// NOTE: it return nil for context done.
func (t *task) genIncrData(pCtx context.Context) (err error) {
	t.logger.Info("generating data for incremental stage")
	getNewCase := true

	defer func() {
		if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded {
			log.L().Info("context done.", log.ShortError(err))
			err = nil // clear error for context done.
		} else if err != nil {
			select {
			case <-pCtx.Done():
				t.logger.Warn("ignore error when generating data for incremental stage", zap.Error(err))
				err = nil // some other errors like `connection is already closed` may also be reported for context done.
			default:
				if forceIgnoreExecSQLError(err) {
					t.logger.Warn("ignore error when generating data for incremental stage", zap.Error(err))
					// we don't known which connection was bad, so simply reset all of them for the next round.
					for _, conn := range t.sourceConns {
						if err2 := conn.resetConn(t.ctx); err2 != nil {
							t.logger.Warn("fail to reset connection", zap.Error(err2))
						}
						err = nil
					}
				}
			}
		}
	}()

	runCaseSQLs := func() error {
		testSQLs := t.caseGenerator.GetSQLs()
		if testSQLs == nil {
			getNewCase = false
			return nil
		}
		for _, testSQL := range testSQLs {
			log.L().Info("execute test case sql", zap.String("ddl", testSQL.statement), zap.Int("source", testSQL.source))
			if err2 := t.sourceConns[testSQL.source].execDDLs(t.ctx, testSQL.statement); err2 != nil {
				return err2
			}
		}
		return nil
	}

	defer func() {
		log.L().Info("complete test case sql")
		for {
			if !getNewCase {
				return
			}

			if err2 := runCaseSQLs(); err2 != nil {
				err = err2
				return
			}
			if err2 := t.updateSchema(); err2 != nil {
				err = err2
				return
			}
		}
	}()

	for {
		select {
		case <-pCtx.Done():
			return nil
		default:
		}

		// for DML, we rand choose an upstream source to execute the statement.
		idx := rand.Intn(len(t.sourceConns))
		query, typ, err := randDML(t.ss[idx])
		if err != nil {
			return err
		}
		if err = t.sourceConns[idx].execDDLs(t.ctx, query); err != nil {
			return err
		}

		switch typ {
		case insertDML:
			t.results[idx].Insert++
		case updateDML:
			t.results[idx].Update++
		case deleteDML:
			t.results[idx].Delete++
		default:
		}

		schemaChanged := false
		if rand.Intn(3000) < 10 {
			query, err = randDDL(t.ss[0])
			if err != nil {
				return err
			}

			// Unsupported ddl in optimistic mode. e.g. ALTER TABLE table_name ADD column column_name INT NOT NULL;
			if t.taskCfg.ShardMode == config2.ShardOptimistic {
				if yes, err2 := isNotNullNonDefaultAddCol(query); err != nil {
					return err2
				} else if yes {
					continue
				}
			}

			t.logger.Info("executing DDL", zap.String("query", query))
			// for DDL, we execute the statement for all upstream sources.
			// NOTE: no re-order inject even for optimistic shard DDL now.

			var eg errgroup.Group
			for i, c := range t.sourceConns {
				conn2 := c
				i2 := i
				eg.Go(func() error {
					if err2 := conn2.execDDLs(t.ctx, query); err2 != nil {
						if conn.IsMySQLError(err2, mysql.ErrDupFieldName) {
							t.logger.Warn("ignore duplicate field name for ddl", log.ShortError(err))
							return nil
						}
						return err2
					}
					t.results[i2].DDL++
					return nil
				})
			}
			if err = eg.Wait(); err != nil {
				return err
			}

			schemaChanged = true
		}

		if getNewCase && rand.Intn(100) < 10 {
			// execute sql of test cases
			if err = runCaseSQLs(); err != nil {
				return err
			}

			schemaChanged = true
		}

		if schemaChanged {
			if err = t.updateSchema(); err != nil {
				return err
			}
		}
	}
}

// diffIncrData checks data equal for the incremental stage in one round.
// NOTE: it return nil for context done.
func (t *task) diffIncrData(ctx context.Context) (err error) {
	t.logger.Info("check data for incremental stage")

	defer func() {
		if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded {
			err = nil // clear error for context done.
		} else if err != nil {
			select {
			case <-ctx.Done():
				t.logger.Warn("ignore error when check data for incremental stage", zap.Error(err))
				err = nil // some other errors like `connection is already closed` may also be reported for context done.
			default:
			}
		}
	}()

	sourceDBs := make([]*sql.DB, 0, len(t.sourceDBs))
	for _, db := range t.sourceDBs {
		sourceDBs = append(sourceDBs, db.DB)
	}
	return diffDataLoop(ctx, diffCount, diffInterval, t.schema, t.tables, t.targetDB.DB, sourceDBs...)
}

func (t *task) updateSchema() error {
	ctx, cancel := context.WithTimeout(context.Background(), conn.DefaultDBTimeout)
	defer cancel()

	for i, db := range t.sourceDBs {
		columns := make([][5]string, 0)
		indexes := make(map[string][]string)
		for _, table := range t.tables {
			createTable, err := dbutil.GetCreateTableSQL(ctx, db.DB, t.schema, table)
			if err != nil {
				return err
			}
			col, idx, err := createTableToSmithSchema(t.schema, createTable)
			if err != nil {
				return err
			}
			columns = append(columns, col...)
			indexes[table] = idx
		}
		t.ss[i] = sqlsmith.New()
		t.ss[i].SetDB(t.schema)
		t.ss[i].LoadSchema(columns, indexes)
	}
	return nil
}

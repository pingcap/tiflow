// Copyright 2022 PingCAP, Inc.
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

package checkpoint

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tidb/util/mock"
	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	router "github.com/pingcap/tidb/util/table-router"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/bootstrap"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	loadCheckpointTable = `CREATE TABLE IF NOT EXISTS %s (
		task_name varchar(255) NOT NULL,
		source_name varchar(255) NOT NULL,
		status varchar(10) NOT NULL DEFAULT 'init' COMMENT 'init,running,finished',
		PRIMARY KEY (task_name, source_name)
	);`
	syncCheckpointTable = `CREATE TABLE IF NOT EXISTS %s (
		id VARCHAR(32) NOT NULL,
		cp_schema VARCHAR(128) NOT NULL,
		cp_table VARCHAR(128) NOT NULL,
		binlog_name VARCHAR(128),
		binlog_pos INT UNSIGNED,
		binlog_gtid TEXT,
		exit_safe_binlog_name VARCHAR(128) DEFAULT '',
		exit_safe_binlog_pos INT UNSIGNED DEFAULT 0,
		exit_safe_binlog_gtid TEXT,
		table_info JSON NOT NULL,
		is_global BOOLEAN,
		create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_id_schema_table (id, cp_schema, cp_table)
	)`
)

// NewCheckpointAgent is a method to create a new checkpoint agent
var NewCheckpointAgent = NewAgentImpl

// Agent defeins a checkpoint agent interface
type Agent interface {
	Create(ctx context.Context, cfg *config.JobCfg) error
	Remove(ctx context.Context, cfg *config.JobCfg) error
	IsFresh(ctx context.Context, workerType framework.WorkerType, task *metadata.Task) (bool, error)
	Upgrade(ctx context.Context, preVer semver.Version) error
	FetchAllDoTables(ctx context.Context, cfg *config.JobCfg) (map[metadata.TargetTable][]metadata.SourceTable, error)
	// FetchTableStmt fetch create table statement from checkpoint.
	FetchTableStmt(ctx context.Context, jobID string, cfg *config.JobCfg, sourceTable metadata.SourceTable) (string, error)
}

// AgentImpl implements Agent
type AgentImpl struct {
	*bootstrap.DefaultUpgrader

	jobID  string
	logger *zap.Logger
}

// NewAgentImpl creates a new AgentImpl instance
func NewAgentImpl(jobID string, pLogger *zap.Logger) Agent {
	logger := pLogger.With(zap.String("component", "checkpoint_agent"))
	c := &AgentImpl{
		DefaultUpgrader: bootstrap.NewDefaultUpgrader(logger),
		jobID:           jobID,
		logger:          logger,
	}
	c.DefaultUpgrader.Upgrader = c
	return c
}

// Create implements Agent.Create
// Create will be called when the job is created/restarted/updated.
// We create checkpoint table in master rather than in workers,
// to avoid the annoying log of "table already exists",
// because one job only need to create one checkpoint table per unit.
// move these codes to tiflow later.
func (c *AgentImpl) Create(ctx context.Context, cfg *config.JobCfg) error {
	c.logger.Info("create checkpoint")
	db, err := conn.GetDownstreamDB(cfg.TargetDB)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()

	if err := createMetaDatabase(ctx, cfg, db); err != nil {
		return errors.Trace(err)
	}

	if cfg.TaskMode != dmconfig.ModeIncrement {
		if err := createLoadCheckpointTable(ctx, c.jobID, cfg, db); err != nil {
			return errors.Trace(err)
		}
	}
	if cfg.TaskMode != dmconfig.ModeFull {
		if err := createSyncCheckpointTable(ctx, c.jobID, cfg, db); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Remove implements Agent.Remove
func (c *AgentImpl) Remove(ctx context.Context, cfg *config.JobCfg) error {
	c.logger.Info("remove checkpoint")
	db, err := conn.GetDownstreamDB(cfg.TargetDB)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()

	if err := dropLoadCheckpointTable(ctx, c.jobID, cfg, db); err != nil {
		return errors.Trace(err)
	}
	return dropSyncCheckpointTable(ctx, c.jobID, cfg, db)
}

// IsFresh implements Agent.IsFresh
func (c *AgentImpl) IsFresh(ctx context.Context, workerType framework.WorkerType, task *metadata.Task) (bool, error) {
	if workerType == frameModel.WorkerDMDump {
		return true, nil
	}

	db, err := conn.GetDownstreamDB(task.Cfg.TargetDB)
	if err != nil {
		return false, err
	}
	defer db.Close()

	if workerType == frameModel.WorkerDMLoad {
		return isLoadFresh(ctx, c.jobID, task.Cfg, db)
	}
	return isSyncFresh(ctx, c.jobID, task.Cfg, db)
}

// UpgradeFuncs implement the Upgrader interface.
func (c *AgentImpl) UpgradeFuncs() []bootstrap.UpgradeFunc {
	return nil
}

// FetchAllDoTables returns all need to do tables after filtered and routed (fetches from upstream MySQL).
func (c *AgentImpl) FetchAllDoTables(ctx context.Context, cfg *config.JobCfg) (map[metadata.TargetTable][]metadata.SourceTable, error) {
	c.logger.Info("fetch all do tables from upstream")

	var (
		mu      sync.Mutex
		result  = make(map[metadata.TargetTable][]metadata.SourceTable, len(cfg.Upstreams))
		g, gCtx = errgroup.WithContext(ctx)
	)

	for _, upstream := range cfg.Upstreams {
		up := upstream
		// filter/route in errgroup will cause data race.
		baList, err := filter.New(up.CaseSensitive, cfg.BAList[up.BAListName])
		if err != nil {
			return result, err
		}
		routeRules := make([]*router.TableRule, 0, len(up.RouteRules))
		for _, ruleName := range up.RouteRules {
			routeRules = append(routeRules, cfg.Routes[ruleName])
		}
		router, err := regexprrouter.NewRegExprRouter(up.CaseSensitive, routeRules)
		if err != nil {
			return result, err
		}

		g.Go(func() error {
			db, err := conn.DefaultDBProvider.Apply(conn.UpstreamDBConfig(up.DBCfg))
			if err != nil {
				return errors.Trace(err)
			}
			defer db.Close()

			// fetch all do tables
			sourceTables, err := conn.FetchAllDoTables(gCtx, db, baList)
			if err != nil {
				return errors.Trace(err)
			}

			for schema, tables := range sourceTables {
				for _, table := range tables {
					targetSchema, targetTable, err := router.Route(schema, table)
					if err != nil {
						return errors.Trace(err)
					}

					target := metadata.TargetTable{Schema: targetSchema, Table: targetTable}
					mu.Lock()
					result[target] = append(result[target], metadata.SourceTable{Source: up.SourceID, Schema: schema, Table: table})
					mu.Unlock()
				}
			}
			return nil
		})

	}
	err := g.Wait()
	return result, err
}

// FetchTableStmt fetch create table statement from checkpoint.
// TODO(https://github.com/pingcap/tiflow/issues/5334): save create table statement to checkpoint instead of table info.
func (c *AgentImpl) FetchTableStmt(ctx context.Context, jobID string, cfg *config.JobCfg, sourceTable metadata.SourceTable) (string, error) {
	c.logger.Info("fetch table info from checkpoint")
	db, err := conn.DefaultDBProvider.Apply(conn.DownstreamDBConfig(cfg.TargetDB))
	if err != nil {
		return "", err
	}
	defer db.Close()

	query := `SELECT table_info FROM ` + syncTableName(jobID, cfg) + ` WHERE id = ? AND cp_schema = ? AND cp_table = ?`
	row := db.DB.QueryRowContext(ctx, query, sourceTable.Source, sourceTable.Schema, sourceTable.Table)
	if row.Err() != nil {
		return "", row.Err()
	}
	var tiBytes []byte
	if err := row.Scan(&tiBytes); err != nil {
		if err == sql.ErrNoRows {
			return "", errors.Errorf("table info not found %v", sourceTable)
		}
		return "", err
	}
	var ti *model.TableInfo
	if bytes.Equal(tiBytes, []byte("null")) {
		return "", errors.Errorf("table info not found %v", sourceTable)
	}
	if err := json.Unmarshal(tiBytes, &ti); err != nil {
		return "", errors.Trace(err)
	}

	result := bytes.NewBuffer(make([]byte, 0, 512))
	err = executor.ConstructResultOfShowCreateTable(mock.NewContext(), ti, autoid.Allocators{}, result)
	if err != nil {
		return "", errors.Trace(err)
	}
	return conn.CreateTableSQLToOneRow(result.String()), nil
}

func createMetaDatabase(ctx context.Context, cfg *config.JobCfg, db *conn.BaseDB) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbutil.ColumnName(cfg.MetaSchema))
	_, err := db.DB.ExecContext(ctx, query)
	return errors.Trace(err)
}

func createLoadCheckpointTable(ctx context.Context, jobID string, cfg *config.JobCfg, db *conn.BaseDB) error {
	_, err := db.DB.ExecContext(ctx, fmt.Sprintf(loadCheckpointTable, loadTableName(jobID, cfg)))
	return errors.Trace(err)
}

func createSyncCheckpointTable(ctx context.Context, jobID string, cfg *config.JobCfg, db *conn.BaseDB) error {
	_, err := db.DB.ExecContext(ctx, fmt.Sprintf(syncCheckpointTable, syncTableName(jobID, cfg)))
	return errors.Trace(err)
}

func dropLoadCheckpointTable(ctx context.Context, jobID string, cfg *config.JobCfg, db *conn.BaseDB) error {
	dropTable := "DROP TABLE IF EXISTS %s"
	_, err := db.DB.ExecContext(ctx, fmt.Sprintf(dropTable, loadTableName(jobID, cfg)))
	return errors.Trace(err)
}

func dropSyncCheckpointTable(ctx context.Context, jobID string, cfg *config.JobCfg, db *conn.BaseDB) error {
	dropTable := "DROP TABLE IF EXISTS %s"
	if _, err := db.DB.ExecContext(ctx, fmt.Sprintf(dropTable, syncTableName(jobID, cfg))); err != nil {
		return errors.Trace(err)
	}
	// The following two would be better removed in the worker when destroy.
	if _, err := db.DB.ExecContext(ctx, fmt.Sprintf(dropTable, shardMetaName(jobID, cfg))); err != nil {
		return errors.Trace(err)
	}
	if _, err := db.DB.ExecContext(ctx, fmt.Sprintf(dropTable, onlineDDLName(jobID, cfg))); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func loadTableName(jobID string, cfg *config.JobCfg) string {
	return dbutil.TableName(cfg.MetaSchema, cputil.LightningCheckpoint(jobID))
}

func syncTableName(jobID string, cfg *config.JobCfg) string {
	return dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(jobID))
}

func shardMetaName(jobID string, cfg *config.JobCfg) string {
	return dbutil.TableName(cfg.MetaSchema, cputil.SyncerShardMeta(jobID))
}

func onlineDDLName(jobID string, cfg *config.JobCfg) string {
	return dbutil.TableName(cfg.MetaSchema, cputil.SyncerOnlineDDL(jobID))
}

func isLoadFresh(ctx context.Context, jobID string, taskCfg *config.TaskCfg, db *conn.BaseDB) (bool, error) {
	// nolint:gosec
	query := fmt.Sprintf("SELECT status FROM %s WHERE `task_name` = ? AND `source_name` = ?", loadTableName(jobID, taskCfg.ToJobCfg()))
	var status string
	err := db.DB.QueryRowContext(ctx, query, jobID, taskCfg.Upstreams[0].SourceID).Scan(&status)
	switch {
	case err == nil:
		return status == "init", nil
	case err == sql.ErrNoRows:
		return true, nil
	default:
		return false, err
	}
}

func isSyncFresh(ctx context.Context, jobID string, taskCfg *config.TaskCfg, db *conn.BaseDB) (bool, error) {
	// nolint:gosec
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE `id` = ? AND `is_global` = true", syncTableName(jobID, taskCfg.ToJobCfg()))
	var status string
	err := db.DB.QueryRowContext(ctx, query, taskCfg.Upstreams[0].SourceID).Scan(&status)
	switch {
	case err == nil:
		return false, nil
	case err == sql.ErrNoRows:
		return true, nil
	default:
		return false, err
	}
}

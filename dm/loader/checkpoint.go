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

package loader

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

// CheckPoint represents checkpoint status.
type CheckPoint interface {
	// Load loads all checkpoints recorded before.
	// because of no checkpoints updated in memory when error occurred
	// when resuming, Load will be called again to load checkpoints
	Load(tctx *tcontext.Context) error

	// GetRestoringFileInfo get restoring data files for table
	GetRestoringFileInfo(db, table string) map[string][]int64

	// GetAllRestoringFileInfo return all restoring files position
	GetAllRestoringFileInfo() map[string][]int64

	// IsTableCreated checks if db / table was created. set `table` to "" when check db
	IsTableCreated(db, table string) bool

	// IsTableFinished query if table has finished
	IsTableFinished(db, table string) bool

	// CalcProgress calculate which table has finished and which table partial restored
	CalcProgress(allFiles map[string]Tables2DataFiles) error

	// Init initialize checkpoint data in tidb
	Init(tctx *tcontext.Context, filename string, endpos int64) error

	// ResetConn resets database connections owned by the Checkpoint
	ResetConn(tctx *tcontext.Context) error

	// Close closes the CheckPoint
	Close()

	// Clear clears all recorded checkpoints
	Clear(tctx *tcontext.Context) error

	// Count returns recorded checkpoints' count
	Count(tctx *tcontext.Context) (int, error)

	// GenSQL generates sql to update checkpoint to DB
	GenSQL(filename string, offset int64) string

	// UpdateOffset keeps `cp.restoringFiles` in memory same with checkpoint in DB,
	// should be called after update checkpoint in DB
	UpdateOffset(filename string, offset int64) error

	// AllFinished returns `true` when all restoring job are finished
	AllFinished() bool
}

type lightingLoadStatus int

const (
	lightningStatusInit lightingLoadStatus = iota
	lightningStatusRunning
	lightningStatusFinished
)

func (s lightingLoadStatus) String() string {
	switch s {
	case lightningStatusInit:
		return "init"
	case lightningStatusRunning:
		return "running"
	case lightningStatusFinished:
		return "finished"
	default:
		panic(fmt.Sprintf("unknown lightning load stauts '%d'", s))
	}
}

func parseLightningLoadStatus(s string) lightingLoadStatus {
	switch s {
	case "running":
		return lightningStatusRunning
	case "finished":
		return lightningStatusFinished
	case "init":
		return lightningStatusInit
	default:
		log.L().Warn("unknown lightning load status, will fallback to init", zap.String("status", s))
		return lightningStatusInit
	}
}

type LightningCheckpointList struct {
	db         *conn.BaseDB
	schema     string
	tableName  string
	taskName   string
	sourceName string
	logger     log.Logger
}

func NewLightningCheckpointList(
	db *conn.BaseDB,
	taskName string,
	sourceName string,
	metaSchema string,
	logger log.Logger,
) *LightningCheckpointList {
	return &LightningCheckpointList{
		db:         db,
		schema:     dbutil.ColumnName(metaSchema),
		tableName:  dbutil.TableName(metaSchema, cputil.LightningCheckpoint(taskName)),
		taskName:   taskName,
		sourceName: sourceName,
		logger:     logger.WithFields(zap.String("component", "lightning checkpoint database list")),
	}
}

func (cp *LightningCheckpointList) Prepare(ctx context.Context) error {
	connection, err := cp.db.GetBaseConn(ctx)
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "initialize connection when prepare"), terror.ScopeDownstream)
	}
	defer cp.db.ForceCloseConnWithoutErr(connection)

	createSchema := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", cp.schema)
	tctx := tcontext.NewContext(ctx, log.With(zap.String("job", "lightning-checkpoint")))
	_, err = connection.ExecuteSQL(tctx, nil, "lightning-checkpoint", []string{createSchema})
	if err != nil {
		return err
	}
	createTable := `CREATE TABLE IF NOT EXISTS %s (
		task_name varchar(255) NOT NULL,
		source_name varchar(255) NOT NULL,
		status varchar(10) NOT NULL DEFAULT 'init' COMMENT 'init,running,finished',
		PRIMARY KEY (task_name, source_name)
	);
`
	sql2 := fmt.Sprintf(createTable, cp.tableName)
	_, err = connection.ExecuteSQL(tctx, nil, "lightning-checkpoint", []string{sql2})
	return terror.WithScope(err, terror.ScopeDownstream)
}

func (cp *LightningCheckpointList) RegisterCheckPoint(ctx context.Context) error {
	connection, err := cp.db.GetBaseConn(ctx)
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "initialize connection"), terror.ScopeDownstream)
	}
	defer cp.db.ForceCloseConnWithoutErr(connection)

	sql := fmt.Sprintf("INSERT IGNORE INTO %s (`task_name`, `source_name`) VALUES (?, ?)", cp.tableName)
	cp.logger.Info("initial checkpoint record",
		zap.String("task", cp.taskName),
		zap.String("source", cp.sourceName))
	args := []interface{}{cp.taskName, cp.sourceName}
	tctx := tcontext.NewContext(ctx, log.With(zap.String("job", "lightning-checkpoint")))
	_, err = connection.ExecuteSQL(tctx, nil, "lightning-checkpoint", []string{sql}, args)
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "initialize checkpoint"), terror.ScopeDownstream)
	}
	return nil
}

func (cp *LightningCheckpointList) UpdateStatus(ctx context.Context, status lightingLoadStatus) error {
	connection, err := cp.db.GetBaseConn(ctx)
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "initialize connection"), terror.ScopeDownstream)
	}
	defer cp.db.ForceCloseConnWithoutErr(connection)

	sql := fmt.Sprintf("UPDATE %s set status = ? WHERE `task_name` = ? AND `source_name` = ?", cp.tableName)
	cp.logger.Info("update lightning loader status",
		zap.String("task", cp.taskName), zap.String("source", cp.sourceName),
		zap.Stringer("status", status))
	tctx := tcontext.NewContext(ctx, log.With(zap.String("job", "lightning-checkpoint")))
	_, err = connection.ExecuteSQL(tctx, nil, "lightning-checkpoint", []string{sql},
		[]interface{}{status.String(), cp.taskName, cp.sourceName})
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "update lightning status"), terror.ScopeDownstream)
	}
	return nil
}

func (cp *LightningCheckpointList) taskStatus(ctx context.Context) (lightingLoadStatus, error) {
	connection, err := cp.db.GetBaseConn(ctx)
	if err != nil {
		return lightningStatusInit, terror.WithScope(terror.Annotate(err, "initialize connection"), terror.ScopeDownstream)
	}
	defer cp.db.ForceCloseConnWithoutErr(connection)

	query := fmt.Sprintf("SELECT status FROM %s WHERE `task_name` = ? AND `source_name` = ?", cp.tableName)
	tctx := tcontext.NewContext(ctx, log.With(zap.String("job", "lightning-checkpoint")))
	// nolint:rowserrcheck
	rows, err := connection.QuerySQL(tctx, query, cp.taskName, cp.sourceName)
	if err != nil {
		return lightningStatusInit, err
	}
	defer rows.Close()
	if rows.Next() {
		var status string
		if err = rows.Scan(&status); err != nil {
			return lightningStatusInit, terror.WithScope(err, terror.ScopeDownstream)
		}
		return parseLightningLoadStatus(status), nil
	}
	// status row doesn't exist, return default value
	return lightningStatusInit, nil
}

// Close implements CheckPoint.Close.
func (cp *LightningCheckpointList) Close() {
	if err := cp.db.Close(); err != nil {
		cp.logger.Error("close checkpoint list db", log.ShortError(err))
	}
}

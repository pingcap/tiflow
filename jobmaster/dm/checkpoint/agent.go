package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
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

type Agent interface {
	Init(ctx context.Context) error
	Remove(ctx context.Context) error
	IsFresh(ctx context.Context, workerType lib.WorkerType, task *metadata.Task) (bool, error)
}

type AgentImpl struct {
	mu  sync.RWMutex
	cfg *config.JobCfg
}

func NewAgentImpl(jobCfg *config.JobCfg) *AgentImpl {
	c := &AgentImpl{}
	c.UpdateConfig(jobCfg)
	return c
}

func (c *AgentImpl) GetConfig() *config.JobCfg {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}

func (c *AgentImpl) UpdateConfig(jobCfg *config.JobCfg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg = jobCfg
}

// We create checkpoint table in master rather than in workers,
// to avoid the annoying log of "table already exists",
// because one job only need to create one checkpoint table per unit.
// move these codes to tiflow later.
func (c *AgentImpl) Init(ctx context.Context) error {
	log.L().Info("init checkpoint", zap.String("job_id", c.cfg.Name))
	cfg := c.GetConfig()
	db, err := conn.DefaultDBProvider.Apply(cfg.TargetDB)
	if err != nil {
		return err
	}
	defer db.Close()

	// NOTE: update-job changes the TaskMode is not supported now.
	if cfg.TaskMode != dmconfig.ModeIncrement {
		if err := createLoadCheckpointTable(ctx, cfg, db); err != nil {
			return err
		}
	}
	if cfg.TaskMode != dmconfig.ModeFull {
		if err := createSyncCheckpointTable(ctx, cfg, db); err != nil {
			return err
		}
	}
	return nil
}

func (c *AgentImpl) Remove(ctx context.Context) error {
	log.L().Info("remove checkpoint", zap.String("job_id", c.cfg.Name))
	cfg := c.GetConfig()
	db, err := conn.DefaultDBProvider.Apply(cfg.TargetDB)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := dropLoadCheckpointTable(ctx, cfg, db); err != nil {
		return err
	}
	return dropSyncCheckpointTable(ctx, cfg, db)
}

func (c *AgentImpl) IsFresh(ctx context.Context, workerType lib.WorkerType, task *metadata.Task) (bool, error) {
	if workerType == lib.WorkerDMDump {
		return true, nil
	}

	db, err := conn.DefaultDBProvider.Apply(task.Cfg.TargetDB)
	if err != nil {
		return false, err
	}
	defer db.Close()

	if workerType == lib.WorkerDMLoad {
		return isLoadFresh(ctx, task.Cfg, db)
	}
	return isSyncFresh(ctx, task.Cfg, db)
}

func createLoadCheckpointTable(ctx context.Context, cfg *config.JobCfg, db *conn.BaseDB) error {
	_, err := db.DB.ExecContext(ctx, loadCheckpointTable, loadTableName(cfg))
	return err
}

func createSyncCheckpointTable(ctx context.Context, cfg *config.JobCfg, db *conn.BaseDB) error {
	_, err := db.DB.ExecContext(ctx, syncCheckpointTable, syncTableName(cfg))
	return err
}

func dropLoadCheckpointTable(ctx context.Context, cfg *config.JobCfg, db *conn.BaseDB) error {
	dropTable := "DROP TABLE IF EXISTS %s"
	_, err := db.DB.ExecContext(ctx, fmt.Sprintf(dropTable, loadTableName(cfg)))
	return err
}

func dropSyncCheckpointTable(ctx context.Context, cfg *config.JobCfg, db *conn.BaseDB) error {
	dropTable := "DROP TABLE IF EXISTS %s"
	if _, err := db.DB.ExecContext(ctx, fmt.Sprintf(dropTable, syncTableName(cfg))); err != nil {
		return err
	}
	// The following two would be better removed in the worker when destroy.
	if _, err := db.DB.ExecContext(ctx, fmt.Sprintf(dropTable, shardMetaName(cfg))); err != nil {
		return err
	}
	if _, err := db.DB.ExecContext(ctx, fmt.Sprintf(dropTable, onlineDDLName(cfg))); err != nil {
		return err
	}
	return nil
}

func loadTableName(cfg *config.JobCfg) string {
	return dbutil.TableName(cfg.MetaSchema, cputil.LightningCheckpoint(cfg.Name))
}

func syncTableName(cfg *config.JobCfg) string {
	return dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))
}

func shardMetaName(cfg *config.JobCfg) string {
	return dbutil.TableName(cfg.MetaSchema, cputil.SyncerShardMeta(cfg.Name))
}

func onlineDDLName(cfg *config.JobCfg) string {
	return dbutil.TableName(cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))
}

func isLoadFresh(ctx context.Context, taskCfg *config.TaskCfg, db *conn.BaseDB) (bool, error) {
	query := "SELECT status FROM %s WHERE `task_name` = ? AND `source_name` = ?"
	var status string
	err := db.DB.QueryRowContext(ctx, query, loadTableName((*config.JobCfg)(taskCfg)), taskCfg.Name, taskCfg.Upstreams[0].SourceID).Scan(&status)
	switch {
	case err == nil:
		return status == "init", nil
	case err == sql.ErrNoRows:
		return true, nil
	default:
		return false, err
	}
}

func isSyncFresh(ctx context.Context, taskCfg *config.TaskCfg, db *conn.BaseDB) (bool, error) {
	query := "SELECT 1 FROM %s WHERE `id` = ? AND `is_global` = true"
	var status string
	err := db.DB.QueryRowContext(ctx, query, syncTableName((*config.JobCfg)(taskCfg)), taskCfg.Upstreams[0].SourceID).Scan(&status)
	switch {
	case err == nil:
		return false, nil
	case err == sql.ErrNoRows:
		return true, nil
	default:
		return false, err
	}
}

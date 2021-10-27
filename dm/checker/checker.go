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

package checker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/conn"
	fr "github.com/pingcap/dm/pkg/func-rollback"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	_ "github.com/go-sql-driver/mysql" // for mysql
	"github.com/pingcap/tidb-tools/pkg/check"
	column "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// the total time needed to complete the check depends on the number of instances, databases and tables,
	// now increase the total timeout to 30min, but set `readTimeout` to 30s for source/target DB.
	// if we can not complete the check in 30min, then we must need to refactor the implementation of the function.
	checkTimeout = 30 * time.Minute
	readTimeout  = "30s"
)

type mysqlInstance struct {
	cfg *config.SubTaskConfig

	sourceDB     *conn.BaseDB
	sourceDBinfo *dbutil.DBConfig

	targetDB     *conn.BaseDB
	targetDBInfo *dbutil.DBConfig
}

// Checker performs pre-check of data synchronization.
type Checker struct {
	closed atomic.Bool

	logger log.Logger

	instances []*mysqlInstance

	checkList     []check.Checker
	checkingItems map[string]string
	result        struct {
		sync.RWMutex
		detail *check.Results
	}
	errCnt  int64
	warnCnt int64
}

// NewChecker returns a checker.
func NewChecker(cfgs []*config.SubTaskConfig, checkingItems map[string]string, errCnt, warnCnt int64) *Checker {
	c := &Checker{
		instances:     make([]*mysqlInstance, 0, len(cfgs)),
		checkingItems: checkingItems,
		logger:        log.With(zap.String("unit", "task check")),
		errCnt:        errCnt,
		warnCnt:       warnCnt,
	}

	for _, cfg := range cfgs {
		// we have verify it in SubTaskConfig.Adjust
		replica, _ := cfg.DecryptPassword()
		c.instances = append(c.instances, &mysqlInstance{
			cfg: replica,
		})
	}

	return c
}

// Init implements Unit interface.
func (c *Checker) Init(ctx context.Context) (err error) {
	rollbackHolder := fr.NewRollbackHolder("checker")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	rollbackHolder.Add(fr.FuncRollback{Name: "close-DBs", Fn: c.closeDBs})

	// target name => source => schema => [tables]
	sharding := make(map[string]map[string]map[string][]string)
	shardingCounter := make(map[string]int)
	dbs := make(map[string]*sql.DB)
	columnMapping := make(map[string]*column.Mapping)
	_, checkingShardID := c.checkingItems[config.ShardAutoIncrementIDChecking]
	_, checkingShard := c.checkingItems[config.ShardTableSchemaChecking]
	_, checkSchema := c.checkingItems[config.TableSchemaChecking]

	for _, instance := range c.instances {
		bw, err := filter.New(instance.cfg.CaseSensitive, instance.cfg.BAList)
		if err != nil {
			return terror.ErrTaskCheckGenBAList.Delegate(err)
		}
		r, err := router.NewTableRouter(instance.cfg.CaseSensitive, instance.cfg.RouteRules)
		if err != nil {
			return terror.ErrTaskCheckGenTableRouter.Delegate(err)
		}

		columnMapping[instance.cfg.SourceID], err = column.NewMapping(instance.cfg.CaseSensitive, instance.cfg.ColumnMappingRules)
		if err != nil {
			return terror.ErrTaskCheckGenColumnMapping.Delegate(err)
		}

		instance.sourceDBinfo = &dbutil.DBConfig{
			Host:     instance.cfg.From.Host,
			Port:     instance.cfg.From.Port,
			User:     instance.cfg.From.User,
			Password: instance.cfg.From.Password,
		}
		dbCfg := instance.cfg.From
		dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(readTimeout)
		instance.sourceDB, err = conn.DefaultDBProvider.Apply(dbCfg)
		if err != nil {
			return terror.WithScope(terror.ErrTaskCheckFailedOpenDB.Delegate(err, instance.cfg.From.User, instance.cfg.From.Host, instance.cfg.From.Port), terror.ScopeUpstream)
		}

		instance.targetDBInfo = &dbutil.DBConfig{
			Host:     instance.cfg.To.Host,
			Port:     instance.cfg.To.Port,
			User:     instance.cfg.To.User,
			Password: instance.cfg.To.Password,
		}
		dbCfg = instance.cfg.To
		dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(readTimeout)
		instance.targetDB, err = conn.DefaultDBProvider.Apply(dbCfg)
		if err != nil {
			return terror.WithScope(terror.ErrTaskCheckFailedOpenDB.Delegate(err, instance.cfg.To.User, instance.cfg.To.Host, instance.cfg.To.Port), terror.ScopeDownstream)
		}

		if _, ok := c.checkingItems[config.VersionChecking]; ok {
			c.checkList = append(c.checkList, check.NewMySQLVersionChecker(instance.sourceDB.DB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.ServerIDChecking]; ok {
			c.checkList = append(c.checkList, check.NewMySQLServerIDChecker(instance.sourceDB.DB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.BinlogEnableChecking]; ok {
			c.checkList = append(c.checkList, check.NewMySQLBinlogEnableChecker(instance.sourceDB.DB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.BinlogFormatChecking]; ok {
			c.checkList = append(c.checkList, check.NewMySQLBinlogFormatChecker(instance.sourceDB.DB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.BinlogRowImageChecking]; ok {
			c.checkList = append(c.checkList, check.NewMySQLBinlogRowImageChecker(instance.sourceDB.DB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.DumpPrivilegeChecking]; ok {
			c.checkList = append(c.checkList, check.NewSourceDumpPrivilegeChecker(instance.sourceDB.DB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.ReplicationPrivilegeChecking]; ok {
			c.checkList = append(c.checkList, check.NewSourceReplicationPrivilegeChecker(instance.sourceDB.DB, instance.sourceDBinfo))
		}

		if !checkingShard && !checkSchema {
			continue
		}

		mapping, err := utils.FetchTargetDoTables(ctx, instance.sourceDB.DB, bw, r)
		if err != nil {
			return err
		}

		err = sameTableNameDetection(mapping)
		if err != nil {
			return err
		}

		checkTables := make(map[string][]string)
		for name, tables := range mapping {
			for _, table := range tables {
				checkTables[table.Schema] = append(checkTables[table.Schema], table.Name)
				if _, ok := sharding[name]; !ok {
					sharding[name] = make(map[string]map[string][]string)
				}
				if _, ok := sharding[name][instance.cfg.SourceID]; !ok {
					sharding[name][instance.cfg.SourceID] = make(map[string][]string)
				}
				if _, ok := sharding[name][instance.cfg.SourceID][table.Schema]; !ok {
					sharding[name][instance.cfg.SourceID][table.Schema] = make([]string, 0, 1)
				}

				sharding[name][instance.cfg.SourceID][table.Schema] = append(sharding[name][instance.cfg.SourceID][table.Schema], table.Name)
				shardingCounter[name]++
			}
		}
		dbs[instance.cfg.SourceID] = instance.sourceDB.DB

		if checkSchema {
			c.checkList = append(c.checkList, check.NewTablesChecker(instance.sourceDB.DB, instance.sourceDBinfo, checkTables))
		}
	}

	if checkingShard {
		for name, shardingSet := range sharding {
			if shardingCounter[name] <= 1 {
				continue
			}

			c.checkList = append(c.checkList, check.NewShardingTablesChecker(name, dbs, shardingSet, columnMapping, checkingShardID))
		}
	}

	c.logger.Info(c.displayCheckingItems())
	return nil
}

func (c *Checker) displayCheckingItems() string {
	if len(c.checkList) == 0 {
		return "not found any checking items\n"
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "\n************ task %s checking items ************\n", c.instances[0].cfg.Name)
	for _, checkFunc := range c.checkList {
		fmt.Fprintf(&buf, "%s\n", checkFunc.Name())
	}
	fmt.Fprintf(&buf, "************ task %s checking items ************", c.instances[0].cfg.Name)
	return buf.String()
}

// Process implements Unit interface.
func (c *Checker) Process(ctx context.Context, pr chan pb.ProcessResult) {
	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()

	isCanceled := false
	errs := make([]*pb.ProcessError, 0, 1)
	result, err := check.Do(cctx, c.checkList)
	if err != nil {
		errs = append(errs, unit.NewProcessError(err))
	} else if !result.Summary.Passed {
		errs = append(errs, unit.NewProcessError(errors.New("check was failed, please see detail")))
		warnLeft, errLeft := c.warnCnt, c.errCnt

		// remove success result if not pass
		results := result.Results[:0]
		for _, r := range result.Results {
			if r.State == check.StateSuccess {
				continue
			}

			// handle results without r.Errors
			if len(r.Errors) == 0 {
				switch r.State {
				case check.StateWarning:
					if warnLeft == 0 {
						continue
					}
					warnLeft--
					results = append(results, r)
				case check.StateFailure:
					if errLeft == 0 {
						continue
					}
					errLeft--
					results = append(results, r)
				}
				continue
			}

			subErrors := make([]*check.Error, 0, len(r.Errors))
			for _, e := range r.Errors {
				switch e.Severity {
				case check.StateWarning:
					if warnLeft == 0 {
						continue
					}
					warnLeft--
					subErrors = append(subErrors, e)
				case check.StateFailure:
					if errLeft == 0 {
						continue
					}
					errLeft--
					subErrors = append(subErrors, e)
				}
			}
			// skip display an empty Result
			if len(subErrors) > 0 {
				r.Errors = subErrors
				results = append(results, r)
			}
		}
		result.Results = results
	}

	c.updateInstruction(result)

	select {
	case <-cctx.Done():
		isCanceled = true
	default:
	}

	rawResult, err := json.MarshalIndent(result, "\t", "\t")
	if err != nil {
		rawResult = []byte(fmt.Sprintf("marshal error %v", err))
	}

	c.result.Lock()
	c.result.detail = result
	c.result.Unlock()

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
		Detail:     rawResult,
	}
}

// updateInstruction updates the check result's Instruction.
func (c *Checker) updateInstruction(result *check.Results) {
	for _, r := range result.Results {
		if r.State == check.StateSuccess {
			continue
		}

		// can't judge by other field, maybe update it later
		if r.Extra == check.AutoIncrementKeyChecking {
			if strings.HasPrefix(r.Instruction, "please handle it by yourself") {
				r.Instruction += ",  refer to https://docs.pingcap.com/tidb-data-migration/stable/shard-merge-best-practices#handle-conflicts-of-auto-increment-primary-key) for details."
			}
		}
	}
}

// Close implements Unit interface.
func (c *Checker) Close() {
	if c.closed.Load() {
		return
	}

	c.closeDBs()

	c.closed.Store(true)
}

func (c *Checker) closeDBs() {
	for _, instance := range c.instances {
		if instance.sourceDB != nil {
			if err := instance.sourceDB.Close(); err != nil {
				c.logger.Error("close source db", zap.Stringer("db", instance.sourceDBinfo), log.ShortError(err))
			}
			instance.sourceDB = nil
		}

		if instance.targetDB != nil {
			if err := instance.targetDB.Close(); err != nil {
				c.logger.Error("close target db", zap.Stringer("db", instance.targetDBInfo), log.ShortError(err))
			}
			instance.targetDB = nil
		}
	}
}

// Pause implements Unit interface.
func (c *Checker) Pause() {
	if c.closed.Load() {
		c.logger.Warn("try to pause, but already closed")
		return
	}
}

// Resume resumes the paused process.
func (c *Checker) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if c.closed.Load() {
		c.logger.Warn("try to resume, but already closed")
		return
	}

	c.Process(ctx, pr)
}

// Update implements Unit.Update.
func (c *Checker) Update(cfg *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}

// Type implements Unit interface.
func (c *Checker) Type() pb.UnitType {
	return pb.UnitType_Check
}

// IsFreshTask implements Unit.IsFreshTask.
func (c *Checker) IsFreshTask() (bool, error) {
	return true, nil
}

// Status implements Unit interface.
func (c *Checker) Status(_ *binlog.SourceStatus) interface{} {
	c.result.RLock()
	res := c.result.detail
	c.result.RUnlock()

	rawResult, err := json.Marshal(res)
	if err != nil {
		rawResult = []byte(fmt.Sprintf("marshal %+v error %v", res, err))
	}

	return &pb.CheckStatus{
		Passed:     res.Summary.Passed,
		Total:      int32(res.Summary.Total),
		Failed:     int32(res.Summary.Failed),
		Successful: int32(res.Summary.Successful),
		Warning:    int32(res.Summary.Warning),
		Detail:     rawResult,
	}
}

// Error implements Unit interface.
func (c *Checker) Error() interface{} {
	return &pb.CheckError{}
}

func sameTableNameDetection(tables map[string][]*filter.Table) error {
	tableNameSets := make(map[string]string)
	var messages []string

	for name := range tables {
		nameL := strings.ToLower(name)
		if nameO, ok := tableNameSets[nameL]; !ok {
			tableNameSets[nameL] = name
		} else {
			messages = append(messages, fmt.Sprintf("same target table %v vs %s", nameO, name))
		}
	}

	if len(messages) > 0 {
		return terror.ErrTaskCheckSameTableName.Generate(messages)
	}

	return nil
}

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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // for mysql
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tidb/lightning/pkg/importer"
	"github.com/pingcap/tidb/lightning/pkg/importer/opts"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	regexprrouter "github.com/pingcap/tidb/pkg/util/regexpr-router"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/checker"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/dumpling"
	fr "github.com/pingcap/tiflow/dm/pkg/func-rollback"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	onlineddl "github.com/pingcap/tiflow/dm/syncer/online-ddl-tools"
	"github.com/pingcap/tiflow/dm/unit"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

	baList *filter.Filter
}

// Checker performs pre-check of data synchronization.
type Checker struct {
	closed atomic.Bool

	tctx *tcontext.Context

	instances []*mysqlInstance

	checkList         []checker.RealChecker
	checkingItems     map[string]string
	dumpWholeInstance bool
	result            struct {
		sync.RWMutex
		detail *checker.Results
	}
	errCnt  int64
	warnCnt int64

	onlineDDL onlineddl.OnlinePlugin

	stCfgs []*config.SubTaskConfig
}

// NewChecker returns a checker.
func NewChecker(cfgs []*config.SubTaskConfig, checkingItems map[string]string, errCnt, warnCnt int64) *Checker {
	c := &Checker{
		instances:     make([]*mysqlInstance, 0, len(cfgs)),
		checkingItems: checkingItems,
		errCnt:        errCnt,
		warnCnt:       warnCnt,
		stCfgs:        cfgs,
	}

	for _, cfg := range cfgs {
		// we have verified it in SubTaskConfig.Adjust
		replica, _ := cfg.DecryptedClone()
		c.instances = append(c.instances, &mysqlInstance{
			cfg: replica,
		})
	}

	return c
}

// tablePairInfo records information about a upstream-downstream(source-target) table pair.
// Members may have repeated meanings but they have different data structure to satisfy different usages.
type tablePairInfo struct {
	// target table -> sourceID -> source tables
	targetTable2SourceTablesMap map[filter.Table]map[string][]filter.Table
	// target database -> target tables under this database
	db2TargetTables map[string][]filter.Table
	// number of sharding tables (source tables) of a target table among all upstreams.
	targetTableShardNum map[filter.Table]int
	// sourceID -> tables of this source in allow-list
	sourceID2SourceTables map[string][]filter.Table
	// sourceID -> databases that contain allow-list tables
	sourceID2InterestedDB []map[string]struct{}
	// sourceID -> target table -> source tables
	sourceID2TableMap map[string]map[filter.Table][]filter.Table
	// target table -> extended columns
	targetTable2ExtendedColumns map[filter.Table][]string
	// byte size of all upstream tables, counting both data and index
	totalDataSize atomic.Int64
}

func (c *Checker) getTablePairInfo(ctx context.Context) (info *tablePairInfo, err error) {
	info = &tablePairInfo{}
	eg, ctx2 := errgroup.WithContext(ctx)

	// do network things concurrently
	tableMapPerUpstream := make([]map[filter.Table][]filter.Table, len(c.instances))
	extendedColumnPerTable := map[filter.Table][]string{}
	extendedColumnPerTableMu := sync.Mutex{}
	for idx := range c.instances {
		i := idx
		eg.Go(func() error {
			tableMapping, extendedColumnM, fetchErr := c.fetchSourceTargetDB(ctx2, c.instances[i])
			if fetchErr != nil {
				return fetchErr
			}
			tableMapPerUpstream[i] = tableMapping
			for table, cols := range extendedColumnM {
				// same target table may come from different upstream instances
				// though they are duplicated they should be the same
				extendedColumnPerTableMu.Lock()
				extendedColumnPerTable[table] = cols
				extendedColumnPerTableMu.Unlock()
			}
			return nil
		})
	}
	if egErr := eg.Wait(); egErr != nil {
		return nil, egErr
	}

	info.targetTable2ExtendedColumns = extendedColumnPerTable
	info.targetTable2SourceTablesMap = make(map[filter.Table]map[string][]filter.Table)
	info.targetTableShardNum = make(map[filter.Table]int)
	info.db2TargetTables = make(map[string][]filter.Table)

	for i, inst := range c.instances {
		mapping := tableMapPerUpstream[i]
		err = sameTableNameDetection(mapping)
		if err != nil {
			return nil, err
		}

		sourceID := inst.cfg.SourceID
		for targetTable, sourceTables := range mapping {
			tablesPerSource, ok := info.targetTable2SourceTablesMap[targetTable]
			if !ok {
				tablesPerSource = make(map[string][]filter.Table)
				info.targetTable2SourceTablesMap[targetTable] = tablesPerSource
			}
			tablesPerSource[sourceID] = append(tablesPerSource[sourceID], sourceTables...)
			info.targetTableShardNum[targetTable] += len(sourceTables)
			info.db2TargetTables[targetTable.Schema] = append(info.db2TargetTables[targetTable.Schema], targetTable)
		}
	}

	info.sourceID2SourceTables = make(map[string][]filter.Table, len(c.instances))
	info.sourceID2InterestedDB = make([]map[string]struct{}, len(c.instances))
	info.sourceID2TableMap = make(map[string]map[filter.Table][]filter.Table, len(c.instances))
	sourceIDs := make([]string, 0, len(c.instances))
	dbs := make(map[string]*conn.BaseDB, len(c.instances))
	for i, inst := range c.instances {
		sourceID := inst.cfg.SourceID
		info.sourceID2InterestedDB[i] = make(map[string]struct{})
		mapping := tableMapPerUpstream[i]
		info.sourceID2TableMap[sourceID] = mapping
		for _, tables := range mapping {
			info.sourceID2SourceTables[sourceID] = append(info.sourceID2SourceTables[sourceID], tables...)
			for _, table := range tables {
				info.sourceID2InterestedDB[i][table.Schema] = struct{}{}
			}
		}
		sourceIDs = append(sourceIDs, sourceID)
		dbs[sourceID] = inst.sourceDB
	}

	if _, ok := c.checkingItems[config.LightningFreeSpaceChecking]; ok &&
		c.stCfgs[0].LoaderConfig.ImportMode == config.LoadModePhysical &&
		config.HasLoad(c.stCfgs[0].Mode) {
		concurrency, err := checker.GetConcurrency(ctx, sourceIDs, dbs, c.stCfgs[0].MydumperConfig.Threads)
		if err != nil {
			return nil, err
		}

		type job struct {
			db     *conn.BaseDB
			schema string
			table  string
		}

		pool := checker.NewWorkerPoolWithContext[job, int64](ctx, func(result int64) {
			info.totalDataSize.Add(result)
		})
		for i := 0; i < concurrency; i++ {
			pool.Go(func(ctx context.Context, job job) (int64, error) {
				return conn.FetchTableEstimatedBytes(
					ctx,
					job.db,
					job.schema,
					job.table,
				)
			})
		}

		for idx := range c.instances {
			for _, sourceTables := range tableMapPerUpstream[idx] {
				for _, sourceTable := range sourceTables {
					pool.PutJob(job{
						db:     c.instances[idx].sourceDB,
						schema: sourceTable.Schema,
						table:  sourceTable.Name,
					})
				}
			}
		}
		err2 := pool.Wait()
		if err2 != nil {
			return nil, err2
		}
	}

	return info, nil
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

	c.tctx = tcontext.NewContext(ctx, log.With(zap.String("unit", "task check")))
	info, err := c.getTablePairInfo(ctx)
	if err != nil {
		return err
	}

	if _, ok := c.checkingItems[config.ConnNumberChecking]; ok {
		if len(c.stCfgs) > 0 {
			// only check the first subtask's config
			// because the Mode is the same across all the subtasks
			// as long as they are derived from the same task config.
			// TODO: check the connections for syncer
			// TODO: check for incremental mode
			if config.HasDump(c.stCfgs[0].Mode) {
				for i, inst := range c.instances {
					c.checkList = append(c.checkList, checker.NewDumperConnNumberChecker(inst.sourceDB, c.stCfgs[i].MydumperConfig.Threads))
				}
			}
			if config.HasLoad(c.stCfgs[0].Mode) {
				c.checkList = append(c.checkList, checker.NewLoaderConnNumberChecker(c.instances[0].targetDB, c.stCfgs))
			}
		}
	}
	// check target DB's privilege
	if _, ok := c.checkingItems[config.TargetDBPrivilegeChecking]; ok {
		c.checkList = append(c.checkList, checker.NewTargetPrivilegeChecker(
			c.instances[0].targetDB.DB,
			c.instances[0].targetDBInfo,
		))
	}
	// sourceID -> DB
	upstreamDBs := make(map[string]*conn.BaseDB)
	for i, instance := range c.instances {
		sourceID := instance.cfg.SourceID
		// init online ddl for checker
		if instance.cfg.OnlineDDL && c.onlineDDL == nil {
			c.onlineDDL, err = onlineddl.NewRealOnlinePlugin(c.tctx, instance.cfg, nil)
			if err != nil {
				return err
			}
			rollbackHolder.Add(fr.FuncRollback{Name: "close-onlineDDL", Fn: c.closeOnlineDDL})
		}
		if _, ok := c.checkingItems[config.VersionChecking]; ok {
			c.checkList = append(c.checkList, checker.NewMySQLVersionChecker(instance.sourceDB.DB, instance.sourceDBinfo))
		}

		upstreamDBs[sourceID] = instance.sourceDB
		if config.HasDump(instance.cfg.Mode) {
			// increment mode needn't check dump privilege
			if _, ok := c.checkingItems[config.DumpPrivilegeChecking]; ok {
				exportCfg := export.DefaultConfig()
				err := dumpling.ParseExtraArgs(&c.tctx.Logger, exportCfg, strings.Fields(instance.cfg.ExtraArgs))
				if err != nil {
					return err
				}
				c.checkList = append(c.checkList, checker.NewSourceDumpPrivilegeChecker(
					instance.sourceDB.DB,
					instance.sourceDBinfo,
					info.sourceID2SourceTables[sourceID],
					exportCfg.Consistency,
					c.dumpWholeInstance,
				))
			}
		} else if !instance.cfg.UseRelay && instance.cfg.Meta != nil {
			checkMetaPos := len(instance.cfg.Meta.BinLogName) > 0 ||
				(instance.cfg.EnableGTID && len(instance.cfg.Meta.BinLogGTID) > 0)
			if _, ok := c.checkingItems[config.MetaPositionChecking]; checkMetaPos && ok {
				c.checkList = append(c.checkList, checker.NewMetaPositionChecker(instance.sourceDB,
					instance.cfg.From,
					instance.cfg.EnableGTID,
					instance.cfg.Meta))
			}
		}
		if config.HasSync(instance.cfg.Mode) {
			// full mode needn't check follows
			if _, ok := c.checkingItems[config.ServerIDChecking]; ok {
				c.checkList = append(c.checkList, checker.NewMySQLServerIDChecker(instance.sourceDB.DB, instance.sourceDBinfo))
			}
			if _, ok := c.checkingItems[config.BinlogEnableChecking]; ok {
				c.checkList = append(c.checkList, checker.NewMySQLBinlogEnableChecker(instance.sourceDB.DB, instance.sourceDBinfo))
			}
			if _, ok := c.checkingItems[config.BinlogFormatChecking]; ok {
				c.checkList = append(c.checkList, checker.NewMySQLBinlogFormatChecker(instance.sourceDB.DB, instance.sourceDBinfo))
			}
			if _, ok := c.checkingItems[config.BinlogRowImageChecking]; ok {
				c.checkList = append(c.checkList, checker.NewMySQLBinlogRowImageChecker(instance.sourceDB.DB, instance.sourceDBinfo))
			}
			if _, ok := c.checkingItems[config.ReplicationPrivilegeChecking]; ok {
				c.checkList = append(c.checkList, checker.NewSourceReplicationPrivilegeChecker(instance.sourceDB.DB, instance.sourceDBinfo))
			}
			if _, ok := c.checkingItems[config.OnlineDDLChecking]; c.onlineDDL != nil && ok {
				c.checkList = append(c.checkList, checker.NewOnlineDDLChecker(instance.sourceDB.DB, info.sourceID2InterestedDB[i], c.onlineDDL, instance.baList))
			}
			if _, ok := c.checkingItems[config.BinlogDBChecking]; ok {
				c.checkList = append(c.checkList, checker.NewBinlogDBChecker(instance.sourceDB, instance.sourceDBinfo, info.sourceID2InterestedDB[i], instance.cfg.CaseSensitive))
			}
		}
	}

	dumpThreads := c.instances[0].cfg.MydumperConfig.Threads
	if _, ok := c.checkingItems[config.TableSchemaChecking]; ok {
		c.checkList = append(c.checkList, checker.NewTablesChecker(
			upstreamDBs,
			c.instances[0].targetDB,
			info.sourceID2TableMap,
			info.targetTable2ExtendedColumns,
			dumpThreads,
		))
	}

	instance := c.instances[0]
	// Not check the sharding tables’ schema when the mode is increment.
	// Because the table schema obtained from `show create table` is not the schema at the point of binlog.
	_, checkingShardID := c.checkingItems[config.ShardAutoIncrementIDChecking]
	_, checkingShard := c.checkingItems[config.ShardTableSchemaChecking]
	if checkingShard && instance.cfg.ShardMode != "" && config.HasDump(instance.cfg.Mode) {
		isFresh, err := c.IsFreshTask()
		if err != nil {
			return err
		}
		if isFresh {
			for targetTable, shardingSet := range info.targetTable2SourceTablesMap {
				if info.targetTableShardNum[targetTable] <= 1 {
					continue
				}
				if instance.cfg.ShardMode == config.ShardPessimistic {
					c.checkList = append(c.checkList, checker.NewShardingTablesChecker(
						targetTable.String(),
						upstreamDBs,
						shardingSet,
						checkingShardID,
						dumpThreads,
					))
				} else {
					c.checkList = append(c.checkList, checker.NewOptimisticShardingTablesChecker(
						targetTable.String(),
						upstreamDBs,
						shardingSet,
						dumpThreads,
					))
				}
			}
		}
	}

	hasLightningPrecheck := false
	for _, item := range config.LightningPrechecks {
		if _, ok := c.checkingItems[item]; ok {
			hasLightningPrecheck = true
			break
		}
	}

	if config.HasLoad(instance.cfg.Mode) &&
		instance.cfg.LoaderConfig.ImportMode == config.LoadModePhysical &&
		hasLightningPrecheck {
		lCfg, err := loader.GetLightningConfig(loader.MakeGlobalConfig(instance.cfg), instance.cfg)
		if err != nil {
			return err
		}
		// Adjust will raise error when this field is empty, so we set any non empty value here.
		lCfg.Mydumper.SourceDir = "noop://"
		err = lCfg.Adjust(ctx)
		if err != nil {
			return err
		}

		cpdb, err := checkpoints.OpenCheckpointsDB(ctx, lCfg)
		if err != nil {
			return err
		}
		targetDB, err := importer.DBFromConfig(ctx, lCfg.TiDB)
		if err != nil {
			return err
		}

		var opts []pdhttp.ClientOption
		tls, err := common.NewTLS(
			lCfg.Security.CAPath,
			lCfg.Security.CertPath,
			lCfg.Security.KeyPath,
			"",
			lCfg.Security.CABytes,
			lCfg.Security.CertBytes,
			lCfg.Security.KeyBytes,
		)
		if err != nil {
			log.L().Fatal("failed to load TLS certificates", zap.Error(err))
		}
		if o := tls.TLSConfig(); o != nil {
			opts = append(opts, pdhttp.WithTLSConfig(o))
		}
		pdClient := pdhttp.NewClient(
			"dm-check", []string{lCfg.TiDB.PdAddr}, opts...)

		targetInfoGetter, err := importer.NewTargetInfoGetterImpl(lCfg, targetDB, pdClient)
		if err != nil {
			return err
		}

		var dbMetas []*mydump.MDDatabaseMeta

		// use downstream table for shard merging
		for db, tables := range info.db2TargetTables {
			mdTables := make([]*mydump.MDTableMeta, 0, len(tables))
			for _, table := range tables {
				mdTables = append(mdTables, &mydump.MDTableMeta{
					DB:   db,
					Name: table.Name,
				})
			}
			dbMetas = append(dbMetas, &mydump.MDDatabaseMeta{
				Name:   db,
				Tables: mdTables,
			})
		}

		builder := importer.NewPrecheckItemBuilder(
			lCfg,
			dbMetas,
			newLightningPrecheckAdaptor(targetInfoGetter, info),
			cpdb,
			pdClient,
		)

		if _, ok := c.checkingItems[config.LightningFreeSpaceChecking]; ok {
			c.checkList = append(c.checkList, checker.NewLightningFreeSpaceChecker(
				info.totalDataSize.Load(), targetInfoGetter))
		}
		if _, ok := c.checkingItems[config.LightningEmptyRegionChecking]; ok {
			lChecker, err := builder.BuildPrecheckItem(precheck.CheckTargetClusterEmptyRegion)
			if err != nil {
				return err
			}
			c.checkList = append(c.checkList, checker.NewLightningEmptyRegionChecker(lChecker))
		}
		if _, ok := c.checkingItems[config.LightningRegionDistributionChecking]; ok {
			lChecker, err := builder.BuildPrecheckItem(precheck.CheckTargetClusterRegionDist)
			if err != nil {
				return err
			}
			c.checkList = append(c.checkList, checker.NewLightningRegionDistributionChecker(lChecker))
		}
		if _, ok := c.checkingItems[config.LightningDownstreamVersionChecking]; ok {
			lChecker, err := builder.BuildPrecheckItem(precheck.CheckTargetClusterVersion)
			if err != nil {
				return err
			}
			c.checkList = append(c.checkList, checker.NewLightningClusterVersionChecker(lChecker))
		}
		if _, ok := c.checkingItems[config.LightningMutexFeatureChecking]; ok {
			lChecker, err := builder.BuildPrecheckItem(precheck.CheckTargetUsingCDCPITR)
			if err != nil {
				return err
			}
			c.checkList = append(c.checkList, checker.NewLightningCDCPiTRChecker(lChecker))
		}
	}

	c.tctx.Logger.Info(c.displayCheckingItems())
	return nil
}

func (c *Checker) fetchSourceTargetDB(
	ctx context.Context,
	instance *mysqlInstance,
) (map[filter.Table][]filter.Table, map[filter.Table][]string, error) {
	bAList, err := filter.New(instance.cfg.CaseSensitive, instance.cfg.BAList)
	if err != nil {
		return nil, nil, terror.ErrTaskCheckGenBAList.Delegate(err)
	}
	instance.baList = bAList
	r, err := regexprrouter.NewRegExprRouter(instance.cfg.CaseSensitive, instance.cfg.RouteRules)
	if err != nil {
		return nil, nil, terror.ErrTaskCheckGenTableRouter.Delegate(err)
	}

	if err != nil {
		return nil, nil, terror.ErrTaskCheckGenColumnMapping.Delegate(err)
	}

	instance.sourceDBinfo = &dbutil.DBConfig{
		Host:     instance.cfg.From.Host,
		Port:     instance.cfg.From.Port,
		User:     instance.cfg.From.User,
		Password: instance.cfg.From.Password,
	}
	dbCfg := instance.cfg.From
	dbCfg.RawDBCfg = dbconfig.DefaultRawDBConfig().SetReadTimeout(readTimeout)
	instance.sourceDB, err = conn.GetUpstreamDB(&dbCfg)
	if err != nil {
		return nil, nil, terror.WithScope(terror.ErrTaskCheckFailedOpenDB.Delegate(err, instance.cfg.From.User, instance.cfg.From.Host, instance.cfg.From.Port), terror.ScopeUpstream)
	}
	instance.targetDBInfo = &dbutil.DBConfig{
		Host:     instance.cfg.To.Host,
		Port:     instance.cfg.To.Port,
		User:     instance.cfg.To.User,
		Password: instance.cfg.To.Password,
	}
	dbCfg = instance.cfg.To
	dbCfg.RawDBCfg = dbconfig.DefaultRawDBConfig().SetReadTimeout(readTimeout)
	instance.targetDB, err = conn.GetDownstreamDB(&dbCfg)
	if err != nil {
		return nil, nil, terror.WithScope(terror.ErrTaskCheckFailedOpenDB.Delegate(err, instance.cfg.To.User, instance.cfg.To.Host, instance.cfg.To.Port), terror.ScopeDownstream)
	}
	return conn.FetchTargetDoTables(ctx, instance.cfg.SourceID, instance.sourceDB, instance.baList, r)
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
	result, err := checker.Do(cctx, c.checkList)
	if err != nil {
		errs = append(errs, unit.NewProcessError(err))
	} else if !result.Summary.Passed {
		errs = append(errs, unit.NewProcessError(errors.New("check was failed, please see detail")))
	}
	warnLeft, errLeft := c.warnCnt, c.errCnt

	// remove success result if not pass
	results := result.Results[:0]
	for _, r := range result.Results {
		if r.State == checker.StateSuccess {
			continue
		}

		// handle results without r.Errors
		if len(r.Errors) == 0 {
			switch r.State {
			case checker.StateWarning:
				if warnLeft == 0 {
					continue
				}
				warnLeft--
				results = append(results, r)
			case checker.StateFailure:
				if errLeft == 0 {
					continue
				}
				errLeft--
				results = append(results, r)
			}
			continue
		}

		subErrors := make([]*checker.Error, 0, len(r.Errors))
		for _, e := range r.Errors {
			switch e.Severity {
			case checker.StateWarning:
				if warnLeft == 0 {
					continue
				}
				warnLeft--
				subErrors = append(subErrors, e)
			case checker.StateFailure:
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

	c.updateInstruction(result)

	select {
	case <-cctx.Done():
		isCanceled = true
	default:
	}

	var rawResult []byte
	if result.Summary.Successful != result.Summary.Total {
		rawResult, err = json.MarshalIndent(result, "\t", "\t")
		if err != nil {
			rawResult = []byte(fmt.Sprintf("marshal error %v", err))
		}
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
func (c *Checker) updateInstruction(result *checker.Results) {
	for _, r := range result.Results {
		if r.State == checker.StateSuccess {
			continue
		}

		// can't judge by other field, maybe update it later
		if r.Extra == checker.AutoIncrementKeyChecking {
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
	c.closeOnlineDDL()

	c.closed.Store(true)
}

func (c *Checker) closeDBs() {
	for _, instance := range c.instances {
		if instance.sourceDB != nil {
			if err := instance.sourceDB.Close(); err != nil {
				c.tctx.Logger.Error("close source db", zap.Stringer("db", instance.sourceDBinfo), log.ShortError(err))
			}
			instance.sourceDB = nil
		}

		if instance.targetDB != nil {
			if err := instance.targetDB.Close(); err != nil {
				c.tctx.Logger.Error("close target db", zap.Stringer("db", instance.targetDBInfo), log.ShortError(err))
			}
			instance.targetDB = nil
		}
	}
}

func (c *Checker) closeOnlineDDL() {
	if c.onlineDDL != nil {
		c.onlineDDL.Close()
		c.onlineDDL = nil
	}
}

// Pause implements Unit interface.
func (c *Checker) Pause() {
	if c.closed.Load() {
		c.tctx.Logger.Warn("try to pause, but already closed")
		return
	}
}

// Resume resumes the paused process.
func (c *Checker) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if c.closed.Load() {
		c.tctx.Logger.Warn("try to resume, but already closed")
		return
	}

	c.Process(ctx, pr)
}

// Update implements Unit.Update.
func (c *Checker) Update(ctx context.Context, cfg *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}

// Type implements Unit interface.
func (c *Checker) Type() pb.UnitType {
	return pb.UnitType_Check
}

// IsFreshTask implements Unit.IsFreshTask.
func (c *Checker) IsFreshTask() (bool, error) {
	instance := c.instances[0]
	checkpointSQLs := []string{
		fmt.Sprintf("SHOW CREATE TABLE %s", dbutil.TableName(instance.cfg.MetaSchema, cputil.LoaderCheckpoint(instance.cfg.Name))),
		fmt.Sprintf("SHOW CREATE TABLE %s", dbutil.TableName(instance.cfg.MetaSchema, cputil.LightningCheckpoint(instance.cfg.Name))),
		fmt.Sprintf("SHOW CREATE TABLE %s", dbutil.TableName(instance.cfg.MetaSchema, cputil.SyncerCheckpoint(instance.cfg.Name))),
	}
	var existCheckpoint bool
	for _, sql := range checkpointSQLs {
		c.tctx.Logger.Info("exec query", zap.String("sql", sql))
		rows, err := instance.targetDB.DB.QueryContext(c.tctx.Ctx, sql)
		if err != nil {
			if conn.IsMySQLError(err, mysql.ErrNoSuchTable) {
				continue
			}
			return false, err
		}
		defer rows.Close()
		if rows.Err() != nil {
			return false, rows.Err()
		}
		existCheckpoint = true
		c.tctx.Logger.Info("exist checkpoint, so don't check sharding tables")
		break
	}
	return !existCheckpoint, nil
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

func sameTableNameDetection(tables map[filter.Table][]filter.Table) error {
	tableNameSets := make(map[string]string)
	var messages []string

	for tbl := range tables {
		name := tbl.String()
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

// lightningPrecheckAdaptor implements the importer.PreRestoreInfoGetter interface.
type lightningPrecheckAdaptor struct {
	importer.TargetInfoGetter
	allTables        map[string]*checkpoints.TidbDBInfo
	sourceDataResult importer.EstimateSourceDataSizeResult
}

func newLightningPrecheckAdaptor(
	targetInfoGetter importer.TargetInfoGetter,
	info *tablePairInfo,
) *lightningPrecheckAdaptor {
	var (
		sourceDataResult importer.EstimateSourceDataSizeResult
		allTables        = make(map[string]*checkpoints.TidbDBInfo)
	)
	if info != nil {
		sourceDataResult.SizeWithIndex = info.totalDataSize.Load()
	}
	for db, tables := range info.db2TargetTables {
		allTables[db] = &checkpoints.TidbDBInfo{
			Name:   db,
			Tables: make(map[string]*checkpoints.TidbTableInfo),
		}
		for _, table := range tables {
			allTables[db].Tables[table.Name] = &checkpoints.TidbTableInfo{
				DB:   db,
				Name: table.Name,
			}
		}
	}
	return &lightningPrecheckAdaptor{
		TargetInfoGetter: targetInfoGetter,
		allTables:        allTables,
		sourceDataResult: sourceDataResult,
	}
}

func (l *lightningPrecheckAdaptor) GetAllTableStructures(ctx context.Context, opts ...opts.GetPreInfoOption) (map[string]*checkpoints.TidbDBInfo, error) {
	// re-use with other checker? or in fact we only use other information than structure?
	return l.allTables, nil
}

func (l *lightningPrecheckAdaptor) ReadFirstNRowsByTableName(ctx context.Context, schemaName string, tableName string, n int) (cols []string, rows [][]types.Datum, err error) {
	return nil, nil, errors.New("not implemented")
}

func (l *lightningPrecheckAdaptor) ReadFirstNRowsByFileMeta(ctx context.Context, dataFileMeta mydump.SourceFileMeta, n int) (cols []string, rows [][]types.Datum, err error) {
	return nil, nil, errors.New("not implemented")
}

func (l *lightningPrecheckAdaptor) EstimateSourceDataSize(ctx context.Context, opts ...opts.GetPreInfoOption) (*importer.EstimateSourceDataSizeResult, error) {
	return &l.sourceDataResult, nil
}

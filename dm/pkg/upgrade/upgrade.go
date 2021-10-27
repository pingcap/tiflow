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

package upgrade

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/cputil"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

// upgrades records all functions used to upgrade from one version to the later version.
var upgrades = []func(cli *clientv3.Client, uctx Context) error{
	upgradeToVer1,
	upgradeToVer2,
	upgradeToVer4,
}

// upgradesBeforeScheduler records all upgrade functions before scheduler start. e.g. etcd key changed.
var upgradesBeforeScheduler = []func(ctx context.Context, cli *clientv3.Client) error{
	upgradeToVer3,
}

// Context is used to pass something to TryUpgrade
// NOTE that zero value of Context is nil, be aware of nil-dereference.
type Context struct {
	context.Context
	SubTaskConfigs map[string]map[string]config.SubTaskConfig
}

// newUpgradeContext creates a Context, avoid nil Context member.
// only used for testing now.
func newUpgradeContext() Context {
	return Context{
		Context:        context.Background(),
		SubTaskConfigs: make(map[string]map[string]config.SubTaskConfig),
	}
}

// TryUpgrade tries to upgrade the cluster from an older version to a new version.
// This methods should have no side effects even calling multiple times.
func TryUpgrade(cli *clientv3.Client, uctx Context) error {
	// 1. get previous version from etcd.
	preVer, _, err := GetVersion(cli)
	log.L().Info("fetch previous version", zap.Any("preVer", preVer))
	if err != nil {
		return err
	}

	// 2. check if any previous version exists.
	if preVer.NotSet() {
		if _, err = PutVersion(cli, MinVersion); err != nil {
			return err
		}
		preVer = MinVersion
	}

	// 3. compare the previous version with the current version.
	if cmp := preVer.Compare(CurrentVersion); cmp == 0 {
		// previous == current version, no need to upgrade.
		return nil
	} else if cmp > 0 {
		// previous >= current version, this often means a older version of DM-master become the leader after started,
		// do nothing for this now.
		return nil
	}

	// 4. do upgrade operations.
	for _, upgrade := range upgrades {
		err = upgrade(cli, uctx)
		if err != nil {
			return err
		}
	}

	// 5. put the current version into etcd.
	_, err = PutVersion(cli, CurrentVersion)
	log.L().Info("upgrade cluster version", zap.Any("version", CurrentVersion), zap.Error(err))
	return err
}

// TryUpgradeBeforeSchedulerStart tries to upgrade the cluster before scheduler start.
// This methods should have no side effects even calling multiple times.
func TryUpgradeBeforeSchedulerStart(ctx context.Context, cli *clientv3.Client) error {
	// 1. get previous version from etcd.
	preVer, _, err := GetVersion(cli)
	log.L().Info("fetch previous version", zap.Any("preVer", preVer))
	if err != nil {
		return err
	}

	// 2. check if any previous version exists.
	if preVer.NotSet() {
		if _, err = PutVersion(cli, MinVersion); err != nil {
			return err
		}
		preVer = MinVersion
	}

	// 3. compare the previous version with the current version.
	if cmp := preVer.Compare(CurrentVersion); cmp == 0 {
		// previous == current version, no need to upgrade.
		return nil
	} else if cmp > 0 {
		// previous >= current version, this often means a older version of DM-master become the leader after started,
		// do nothing for this now.
		return nil
	}

	// 4. do upgrade operations.
	for _, upgrade := range upgradesBeforeScheduler {
		err = upgrade(ctx, cli)
		if err != nil {
			return err
		}
	}
	return nil
}

// UntouchVersionUpgrade runs all upgrade functions but doesn't change cluster version. This function is called when
// upgrade from v1.0, with a later PutVersion in caller after success.
func UntouchVersionUpgrade(cli *clientv3.Client, uctx Context) error {
	for _, upgrade := range upgrades {
		err := upgrade(cli, uctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// upgradeToVer1 does upgrade operations from Ver0 to Ver1.
// in fact, this do nothing now, and just for demonstration.
func upgradeToVer1(cli *clientv3.Client, uctx Context) error {
	return nil
}

// upgradeToVer2 does upgrade operations from Ver1 to Ver2 (v2.0.0-GA) to upgrade syncer checkpoint schema.
func upgradeToVer2(cli *clientv3.Client, uctx Context) error {
	upgradeTaskName := "upgradeToVer2"
	logger := log.L().WithFields(zap.String("task", upgradeTaskName))

	if uctx.SubTaskConfigs == nil {
		logger.Info("no downstream DB, skipping")
		return nil
	}

	// tableName -> DBConfig
	dbConfigs := map[string]config.DBConfig{}
	for task, m := range uctx.SubTaskConfigs {
		for sourceID, subCfg := range m {
			tableName := dbutil.TableName(subCfg.MetaSchema, cputil.SyncerCheckpoint(subCfg.Name))
			subCfg2, err := subCfg.DecryptPassword()
			if err != nil {
				log.L().Error("subconfig error when upgrading", zap.String("task", task),
					zap.String("source id", sourceID), zap.String("subtask config", subCfg.String()), zap.Error(err))
				return err
			}
			dbConfigs[tableName] = subCfg2.To
		}
	}

	toClose := make([]*conn.BaseDB, 0, len(dbConfigs))
	defer func() {
		for _, db := range toClose {
			db.Close()
		}
	}()

	// 10 seconds for each subtask
	timeout := time.Duration(len(dbConfigs)*10) * time.Second
	upgradeCtx, cancel := context.WithTimeout(context.Background(), timeout)
	uctx.Context = upgradeCtx
	defer cancel()

	for tableName, cfg := range dbConfigs {
		targetDB, err := conn.DefaultDBProvider.Apply(cfg)
		if err != nil {
			logger.Error("target DB error when upgrading", zap.String("table name", tableName))
			return err
		}
		toClose = append(toClose, targetDB)
		// try to add columns.
		// NOTE: ignore already exists error to continue the process.
		queries := []string{
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN exit_safe_binlog_name VARCHAR(128) DEFAULT '' AFTER binlog_gtid`, tableName),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN exit_safe_binlog_pos INT UNSIGNED DEFAULT 0 AFTER exit_safe_binlog_name`, tableName),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN exit_safe_binlog_gtid TEXT AFTER exit_safe_binlog_pos`, tableName),
		}
		tctx := tcontext.NewContext(uctx.Context, logger)
		dbConn, err := targetDB.GetBaseConn(tctx.Ctx)
		if err != nil {
			logger.Error("skip target DB when upgrading", zap.String("table name", tableName))
			return err
		}
		_, err = dbConn.ExecuteSQLWithIgnoreError(tctx, nil, upgradeTaskName, utils.IgnoreErrorCheckpoint, queries)
		if err != nil {
			logger.Error("error while adding column for checkpoint table", zap.String("table name", tableName))
			return err
		}
	}

	return nil
}

// upgradeToVer3 does upgrade operations from Ver2 (v2.0.0-GA) to Ver3 (v2.0.2) to upgrade etcd key encodings.
// This func should be called before scheduler start.
func upgradeToVer3(ctx context.Context, cli *clientv3.Client) error {
	etcdKeyUpgrades := []struct {
		old common.KeyAdapter
		new common.KeyAdapter
	}{
		{
			common.UpstreamConfigKeyAdapterV1,
			common.UpstreamConfigKeyAdapter,
		},
		{
			common.StageRelayKeyAdapterV1,
			common.StageRelayKeyAdapter,
		},
	}

	ops := make([]clientv3.Op, 0, len(etcdKeyUpgrades))
	for _, pair := range etcdKeyUpgrades {
		resp, err := cli.Get(ctx, pair.old.Path(), clientv3.WithPrefix())
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			log.L().Info("no old KVs, skipping", zap.String("etcd path", pair.old.Path()))
			continue
		}
		for _, kv := range resp.Kvs {
			keys, err2 := pair.old.Decode(string(kv.Key))
			if err2 != nil {
				return err2
			}
			newKey := pair.new.Encode(keys...)

			// note that we lost CreateRevision, Lease, ModRevision, Version
			ops = append(ops, clientv3.OpPut(newKey, string(kv.Value)))
		}
		// delete old key to provide idempotence
		ops = append(ops, clientv3.OpDelete(pair.old.Path(), clientv3.WithPrefix()))
	}
	_, _, err := etcdutil.DoOpsInOneTxnWithRetry(cli, ops...)
	return err
}

// upgradeToVer4 does nothing, version 4 is just to make sure cluster from version 3 could re-run bootstrap, because
// version 3 (v2.0.2) has some bugs and user may downgrade.
func upgradeToVer4(cli *clientv3.Client, uctx Context) error {
	return nil
}

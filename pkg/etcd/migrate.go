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

package etcd

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	clientV3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	// cdcMetaVersion is hard code value indicate the metaVersion of TiCDC
	cdcMetaVersion          = 1
	etcdSessionTTL          = 10
	campaignTimeoutDuration = 1 * time.Minute
	noMetaVersion           = -1
	migrateLogsWarnDuration = 5 * time.Second
	migrationCampaignKey    = "ticdc-migration"
)

type keys map[string]string

func (k keys) addPair(old, new string) {
	k[old] = new
}

type migrator struct {
	clusterID      string
	oldMetaVersion int
	newMetaVersion int
	metaVersionKey string
	// cdc old owner key
	oldOwnerKey string
	// etcd client
	cli *Client
	// all keyPrefixes needed to be migrated or update
	// map from oldKeyPrefix to newKeyPrefix
	keyPrefixes       keys
	etcdNoMetaVersion bool
}

// Note: we do not use etcd transaction to migrate key
// as it has the maximum operation limit in a single transaction.
// So we use a double check mechanism to make sure the migration is complete.
// 1. check and put metaVersion
// 2. campaign old owner
// 3. update keys
// 4. check meta data consistency
// 5. update metaVersion
func (m migrator) migrate(ctx context.Context) error {
	// 1.1 check metaVersion, if the metaVersion in etcd does not match
	// m.oldMetaVersion, it means that someone has migrated the meta data
	metaVersion, err := getMetaVersion(ctx, m.cli, m.clusterID)
	if err != nil {
		log.Error("get meta version failed, etcd meta data migration failed", zap.Error(err))
		return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
	}

	if metaVersion > m.newMetaVersion {
		log.Panic("meta version in etcd is greater than the meta version in TiCDC",
			zap.Int("etcdMetaVersion", metaVersion), zap.Int("cdcMetaVersion", m.newMetaVersion))
	}

	// if metaVersion in etcd is equal to m.newMetaVersion,
	// it means that there is no need to migrate
	if !m.etcdNoMetaVersion && metaVersion == m.newMetaVersion {
		log.Warn("meta version no match, no need to migrate")
		return nil
	}

	// 1.2 put metaVersionKey to etcd to panic old version cdc server
	if m.etcdNoMetaVersion {
		_, err := m.cli.Put(ctx, m.metaVersionKey, fmt.Sprintf("%d", m.oldMetaVersion))
		if err != nil {
			log.Error("put meta version failed, etcd meta data migration failed", zap.Error(err))
			return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
		}
	}

	// 3.campaign old owner to make sure old keys will not be updates
	campaignCtx, cancel := context.WithTimeout(ctx, campaignTimeoutDuration)
	defer cancel()
	if err := m.campaignOldOwner(campaignCtx); err != nil {
		if errors.ErrorEqual(err, context.DeadlineExceeded) {
			log.Error("campaign old owner timeout",
				zap.Duration("duration", campaignTimeoutDuration))
		}
		log.Error("campaign old owner failed, etcd meta data migration failed",
			zap.Error(err))
		return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
	}

	beforeKV := make(map[string][]byte)
	// 4.campaign owner successfully, begin to migrate data
	for oldPrefix, newPrefix := range m.keyPrefixes {
		resp, err := m.cli.Get(ctx, oldPrefix, clientV3.WithPrefix())
		if err != nil {
			log.Error("get old meta data failed, etcd meta data migration failed",
				zap.Error(err))
			return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
		}
		for _, v := range resp.Kvs {
			oldKey := string(v.Key)
			newKey := newPrefix + oldKey[len(oldPrefix):]
			beforeKV[newKey] = v.Value
			log.Info("migrate key", zap.String("oldKey", oldKey), zap.String("newKey", newKey))
			// TODO(dongmen): we should update value after changefeedInfo struct altered
			_, err := m.cli.Put(ctx, newKey, string(v.Value))
			if err != nil {
				log.Error("put new meta data failed, etcd meta data migration failed",
					zap.Error(err))
				return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
			}
		}
	}
	// 5. get all migrated data
	afterKV := make(map[string][]byte)
	for _, newPrefix := range m.keyPrefixes {
		resp, err := m.cli.Get(ctx, newPrefix, clientV3.WithPrefix())
		if err != nil {
			log.Error("get old meta data failed, etcd meta data migration failed", zap.Error(err))
			return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
		}
		for _, v := range resp.Kvs {
			afterKV[string(v.Key)] = v.Value
		}
	}

	// 6. check if data is same before and after migrate
	if !reflect.DeepEqual(beforeKV, afterKV) {
		return cerror.ErrEtcdMigrateFailed.GenWithStackByArgs(
			"etcd meta data do not consistent between before and after migration")
	}

	// 7. update metaVersion
	_, err = m.cli.Put(ctx, m.metaVersionKey, fmt.Sprintf("%d", m.newMetaVersion))
	if err != nil {
		log.Error("update meta version failed, etcd meta data migration failed", zap.Error(err))
		return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
	}

	log.Info("etcd data migration successful")
	return nil
}

func (m migrator) campaignOldOwner(ctx context.Context) error {
	sess, err := concurrency.NewSession(m.cli.Unwrap(),
		concurrency.WithTTL(etcdSessionTTL))
	if err != nil {
		return errors.Trace(err)
	}
	election := concurrency.NewElection(sess, m.oldOwnerKey)
	defer func() {
		_ = sess.Close()
	}()

	if err := election.Campaign(ctx, migrationCampaignKey); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// MigrateData migrate etcd meta data
func MigrateData(ctx context.Context, cli *Client, clusterID string) error {
	version, err := getMetaVersion(ctx, cli, clusterID)
	if err != nil {
		return errors.Trace(err)
	}

	shouldMigrate := false
	oldVersion, newVersion := 0, cdcMetaVersion

	if version == noMetaVersion {
		shouldMigrate = true
	} else {
		oldVersion = version
		shouldMigrate = oldVersion < newVersion
	}

	if !shouldMigrate {
		return nil
	}

	m := migrator{
		clusterID:         clusterID,
		oldMetaVersion:    oldVersion,
		newMetaVersion:    cdcMetaVersion,
		metaVersionKey:    DefaultClusterAndMetaPrefix + metaVersionKey,
		oldOwnerKey:       "/ticdc/cdc/owner",
		cli:               cli,
		keyPrefixes:       make(keys),
		etcdNoMetaVersion: version == noMetaVersion,
	}

	m.keyPrefixes.addPair("/tidb/cdc/changefeed/info",
		DefaultClusterAndNamespacePrefix+changefeedInfoKey)
	m.keyPrefixes.addPair("/tidb/cdc/job",
		DefaultClusterAndNamespacePrefix+changefeedStatusKey)

	return m.migrate(ctx)
}

// ShouldMigrate checks if we should migrate etcd meta data
func ShouldMigrate(ctx context.Context, cli *Client, clusterID string) (bool, error) {
	version, err := getMetaVersion(ctx, cli, clusterID)
	if err != nil {
		return false, errors.Trace(err)
	}
	return version != cdcMetaVersion, nil
}

// WaitMetaVersionMatched checks and waits until the metaVersion in etcd
// matched to lock cdcMetaVersion
func WaitMetaVersionMatched(ctx context.Context, cli *Client, clusterID string) error {
	version, err := getMetaVersion(ctx, cli, clusterID)
	if err != nil {
		return errors.Trace(err)
	}
	if version == cdcMetaVersion {
		return nil
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	warnLogTicker := time.NewTicker(migrateLogsWarnDuration)
	defer warnLogTicker.Stop()
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			version, err := getMetaVersion(ctx, cli, clusterID)
			if err != nil {
				return errors.Trace(err)
			}
			if version == cdcMetaVersion {
				return nil
			}
		case <-warnLogTicker.C:
			log.Warn("meta data migrating last too long",
				zap.Duration("duration", time.Since(start)))
		}
	}
}

func getMetaVersion(ctx context.Context, cli *Client, clusterID string) (int, error) {
	key := CDCKey{Tp: CDCKeyTypeMetaVersion, ClusterID: clusterID}
	resp, err := cli.Get(ctx, key.String())
	if err != nil {
		return 0, errors.Trace(err)
	}
	// means there no metaVersion in etcd
	if len(resp.Kvs) == 0 {
		return noMetaVersion, nil
	}

	version, err := strconv.Atoi(string(resp.Kvs[0].Value))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return version, nil
}

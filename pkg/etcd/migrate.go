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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	clientV3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/zap"
)

const (
	// cdcMetaVersion is hard code value indicate the metaVersion of TiCDC
	cdcMetaVersion       = 1
	etcdSessionTTL       = 10
	noMetaVersion        = -1
	migrationCampaignKey = "ticdc-migration"
)

type keys map[string]string

func (k keys) addPair(old, new string) {
	k[old] = new
}

type migrater struct {
	oldMetaVersion int
	newMetaVersion int
	metaVersionKey string
	// cdc old onwer key
	oldOwnerKey string
	// etcd client
	cli *Client
	// all keyPrefixs need to be migrated or update
	// map from oldKeyPrefix to newKeyPrefix
	keyPrefixs        keys
	etcdNoMetaVersion bool
}

// Note: we do not use etcd transaction to migrate key
// as it has the maximum operation limit in a single transaction.
// So we use a double check mechanism to make sure the migration is complete.
// 1. check and put metaVersion
// 2. campaign old owner
// 3. update keys
// 4. update metaVersion
func (m migrater) migrate(ctx context.Context) error {
	// 1.1 check metaVersion, if the metaVersion in etcd does not match
	// m.oldMetaVersion, it means that someone has migrated the meta data
	metaVersion, err := getMetaVersion(ctx, m.cli)
	if err != nil {
		log.Error("get meta version failed, etcd meta data migration failed", zap.Error(err))
		return errors.Trace(err)
	}
	if !m.etcdNoMetaVersion && metaVersion != m.oldMetaVersion {
		log.Warn("meta version no match, no need to migrate")
		return nil
	}

	// 1.2 put metaVersionKey to etcd to panic old version cdc server
	if m.etcdNoMetaVersion {
		_, err := m.cli.Put(ctx, m.metaVersionKey, fmt.Sprintf("%d", m.oldMetaVersion))
		if err != nil {
			log.Error("put meta version failed, etcd meta data migration failed", zap.Error(err))
			return errors.Trace(err)
		}
	}

	// 3.campaign old owner to make sure old keys will not be updates
	if err := m.campaignOldOwner(ctx); err != nil {
		log.Error("campaign old owner failed, etcd meta data migration failed", zap.Error(err))
		return errors.Trace(err)
	}

	// 4.campaign owner successfully, begin to migrate data
	for oldPrefix, newPrefix := range m.keyPrefixs {
		resp, err := m.cli.Get(ctx, oldPrefix, clientV3.WithPrefix())
		if err != nil {
			log.Error("get old meta data failed, etcd meta data migration failed", zap.Error(err))
			return errors.Trace(err)
		}
		for _, v := range resp.Kvs {
			oldKey := string(v.Key)
			newKey := newPrefix + oldKey[len(oldPrefix):]
			// TODO(dongmen): we should update value after changedfeedInfo struct altered
			_, err := m.cli.Put(ctx, newKey, string(v.Value))
			if err != nil {
				log.Error("put new meta data failed, etcd meta data migration failed", zap.Error(err))
				return errors.Trace(err)
			}
		}
	}

	// 5. update meataVersion
	_, err = m.cli.Put(ctx, m.metaVersionKey, fmt.Sprintf("%d", m.newMetaVersion))
	if err != nil {
		log.Error("update meta version failed, etcd meta data migration failed", zap.Error(err))
		return errors.Trace(err)
	}

	log.Info("etcd data migration successful")
	return nil
}

func (m migrater) campaignOldOwner(ctx context.Context) error {
	sess, err := concurrency.NewSession(m.cli.Unwrap(),
		concurrency.WithTTL(etcdSessionTTL))
	if err != nil {
		return errors.Trace(err)
	}
	election := concurrency.NewElection(sess, m.oldOwnerKey)
	defer func() {
		_ = sess.Close()
	}()

	// TODO(dongmen): do more investigration about how to handle error here
	if err := election.Campaign(ctx, migrationCampaignKey); err != nil {
		switch errors.Cause(err) {
		case context.Canceled, mvcc.ErrCompacted:
		default:
			// if campaign owner failed, restart capture
			log.Warn("campaign owner failed", zap.Error(err))
		}
		return errors.Trace(err)
	}
	return nil
}

func MigrateData(ctx context.Context, cli *Client) error {
	version, err := getMetaVersion(ctx, cli)
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

	m := migrater{
		oldMetaVersion:    oldVersion,
		newMetaVersion:    cdcMetaVersion,
		metaVersionKey:    DefaultClusterAndMetaPrefix + metaVersionKey,
		oldOwnerKey:       "/ticdc/cdc/owner",
		cli:               cli,
		keyPrefixs:        make(keys),
		etcdNoMetaVersion: version == noMetaVersion,
	}

	m.keyPrefixs.addPair("/tidb/cdc/changefeed/info", DefaultClusterAndNamespacePrefix+changefeedInfoKey)
	m.keyPrefixs.addPair("/tidb/cdc/job", DefaultClusterAndNamespacePrefix+changefeedStatusKey)

	return m.migrate(ctx)
}

func ShouldMigrate(ctx context.Context, cli *Client) (bool, error) {
	version, err := getMetaVersion(ctx, cli)
	if err != nil {
		return false, errors.Trace(err)
	}
	return version != cdcMetaVersion, nil
}

// WaitMetaVersionMatched checks and waits until the metaVersion in etcd
// matched to loack cdcMetaVersion
func WaitMetaVersionMatched(ctx context.Context, cli *Client) error {
	tick := time.NewTicker(time.Second)
	version, err := getMetaVersion(ctx, cli)
	if err != nil {
		return errors.Trace(err)
	}
	if version == cdcMetaVersion {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			version, err := getMetaVersion(ctx, cli)
			if err != nil {
				return errors.Trace(err)
			}
			if version == cdcMetaVersion {
				return nil
			}
		}
	}
}

func getMetaVersion(ctx context.Context, cli *Client) (int, error) {
	key := DefaultClusterAndMetaPrefix + metaVersionKey
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// means there no metaVersion in etcd
	if len(resp.Kvs) == 0 {
		return noMetaVersion, nil
	} else {
		version, err := strconv.Atoi(string(resp.Kvs[0].Value))
		if err != nil {
			return 0, errors.Trace(err)
		}
		return version, nil
	}
}

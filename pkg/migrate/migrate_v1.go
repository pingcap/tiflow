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

package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	pd "github.com/tikv/pd/client"
	clientV3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type keys map[string]string

func (k keys) addPair(old, new string) {
	k[old] = new
}

const oldOwnerKey = "/ticdc/cdc/owner"

// Note: we do not use etcd transaction to migrate key
// as it has the maximum operation limit in a single transaction.
// So we use a double check mechanism to make sure the migration is complete.
// 1. check and put metaVersion
// 2. campaign old owner
// 3. update keys
var _ = addMigrateRecord(1, func(ctx context.Context, m *migrator) error {
	// all keyPrefixes needed to be migrated or update
	// map from oldKeyPrefix to newKeyPrefix
	keyPrefixes := make(keys)
	keyPrefixes.addPair("/tidb/cdc/changefeed/info",
		etcd.DefaultClusterAndNamespacePrefix+etcd.ChangefeedInfoKey)
	keyPrefixes.addPair("/tidb/cdc/job",
		etcd.DefaultClusterAndNamespacePrefix+etcd.ChangefeedStatusKey)

	pdClient, err := m.createPDClientFunc(ctx,
		m.pdEndpoints, m.config.Security)
	if err != nil {
		return errors.Trace(err)
	}
	defer pdClient.Close()

	upstreamID := pdClient.GetClusterID(ctx)
	// 1.1 check metaVersion, if the metaVersion in etcd does not match
	// m.oldMetaVersion, it means that someone has migrated the metadata
	metaVersion, err := getMetaVersion(ctx, m.cli.Client, m.cli.ClusterID)
	etcdNoMetaVersion := metaVersion == noMetaVersion
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
	if !etcdNoMetaVersion && metaVersion == m.newMetaVersion {
		log.Warn("meta version no match, no need to migrate")
		return nil
	}

	// 1.2 put metaVersionKey to etcd to panic old version cdc server
	if etcdNoMetaVersion {
		_, err := m.cli.Client.Put(ctx, m.metaVersionKey, fmt.Sprintf("%d", -1))
		if err != nil {
			log.Error("put meta version failed, etcd meta data migration failed", zap.Error(err))
			return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
		}
	}

	// 3.campaign old owner to make sure old keys will not be updates
	campaignCtx, cancel := context.WithTimeout(ctx, campaignTimeoutDuration)
	defer cancel()
	if err := campaignOldOwner(campaignCtx, m.cli.Client, oldOwnerKey); err != nil {
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
	for oldPrefix, newPrefix := range keyPrefixes {
		resp, err := m.cli.Client.Get(ctx, oldPrefix, clientV3.WithPrefix())
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
			if strings.HasPrefix(string(v.Key), oldChangefeedPrefix) {
				info := new(model.ChangeFeedInfo)
				err = info.Unmarshal(v.Value)
				if err != nil {
					log.Error("unmarshal changefeed failed",
						zap.Error(err))
					return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
				}
				info.UpstreamID = upstreamID
				var str string
				str, err = info.Marshal()
				if err != nil {
					log.Error("marshal changefeed failed",
						zap.Error(err))
					return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
				}
				_, err = m.cli.Client.Put(ctx, newKey, str)
			} else {
				_, err = m.cli.Client.Put(ctx, newKey, string(v.Value))
			}
			if err != nil {
				log.Error("put new meta data failed, etcd meta data migration failed",
					zap.Error(err))
				return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
			}
		}
	}

	err = m.migrateGcServiceSafePointFunc(ctx, pdClient,
		m.config.Security, m.cli.GetGCServiceID(), m.config.GcTTL)
	if err != nil {
		log.Error("update meta version failed, etcd meta data migration failed", zap.Error(err))
		return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
	}
	return nil
})

func migrateGcServiceSafePoint(ctx context.Context,
	pdClient pd.Client,
	config *security.Credential,
	newGcServiceID string,
	ttl int64,
) error {
	gcServiceSafePoins, err := pdutil.ListGcServiceSafePoint(ctx, pdClient,
		config)
	if err != nil {
		log.Error("list gc service safepoint failed",
			zap.Error(err))
		return errors.Trace(err)
	}
	var cdcGcSafePoint *pdutil.ServiceSafePoint
	for _, item := range gcServiceSafePoins.ServiceGCSafepoints {
		if item.ServiceID == oldGcServiceID {
			cdcGcSafePoint = item
		}
	}
	if cdcGcSafePoint != nil {
		_, err := gc.SetServiceGCSafepoint(ctx, pdClient, newGcServiceID,
			ttl,
			cdcGcSafePoint.SafePoint)
		if err != nil {
			log.Error("set gc service safepoint failed",
				zap.Error(err))
			return errors.Trace(err)
		}
		err = gc.RemoveServiceGCSafepoint(ctx, pdClient, oldGcServiceID)
		if err != nil {
			log.Warn("remove old gc safepoint failed", zap.Error(err))
		}
	}
	return nil
}

func campaignOldOwner(ctx context.Context,
	cli *etcd.Client, key string,
) error {
	sess, err := concurrency.NewSession(cli.Unwrap(),
		concurrency.WithTTL(etcdSessionTTL))
	if err != nil {
		return errors.Trace(err)
	}
	election := concurrency.NewElection(sess, key)
	defer func() {
		_ = sess.Close()
	}()

	if err := election.Campaign(ctx, migrationCampaignKey); err != nil {
		return errors.Trace(err)
	}
	return nil
}

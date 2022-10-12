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
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	pd "github.com/tikv/pd/client"
	clientV3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	// cdcMetaVersion is hard code value indicate the metaVersion of TiCDC
	cdcMetaVersion          = 1
	etcdSessionTTL          = 10
	campaignTimeoutDuration = 1 * time.Minute
	noMetaVersion           = -1
	migrateLogsWarnDuration = 5 * time.Second
	migrationCampaignKey    = "ticdc-migration"
	oldChangefeedPrefix     = "/tidb/cdc/changefeed/info"
	oldGcServiceID          = "ticdc"
)

type keys map[string]string

func (k keys) addPair(old, new string) {
	k[old] = new
}

// Migrator migrates the cdc metadata
type Migrator interface {
	// ShouldMigrate checks if we need to migrate metadata
	ShouldMigrate(ctx context.Context) (bool, error)
	// Migrate migrates the cdc metadata
	Migrate(ctx context.Context) error
	// WaitMetaVersionMatched wait util migration is done
	WaitMetaVersionMatched(ctx context.Context) error
	// MarkMigrateDone marks migration is done
	MarkMigrateDone()
	// IsMigrateDone check if migration is done
	IsMigrateDone() bool
}

type migrator struct {
	// oldMetaVersion int
	newMetaVersion int
	metaVersionKey string
	// cdc old owner key
	oldOwnerKey string
	// etcd client
	cli etcd.CDCEtcdClient
	// all keyPrefixes needed to be migrated or update
	// map from oldKeyPrefix to newKeyPrefix
	keyPrefixes keys

	done atomic.Bool

	pdEndpoints []string
	config      *config.ServerConfig

	createPDClientFunc func(ctx context.Context,
		pdEndpoints []string,
		conf *security.Credential) (pd.Client, error)
}

// NewMigrator returns a cdc metadata
func NewMigrator(cli etcd.CDCEtcdClient,
	pdEndpoints []string,
	serverConfig *config.ServerConfig,
) Migrator {
	metaVersionCDCKey := &etcd.CDCKey{
		Tp:        etcd.CDCKeyTypeMetaVersion,
		ClusterID: cli.GetClusterID(),
	}
	return &migrator{
		newMetaVersion:     cdcMetaVersion,
		metaVersionKey:     metaVersionCDCKey.String(),
		oldOwnerKey:        "/ticdc/cdc/owner",
		cli:                cli,
		keyPrefixes:        make(keys),
		pdEndpoints:        pdEndpoints,
		config:             serverConfig,
		createPDClientFunc: createPDClient,
	}
}

// MarkMigrateDone marks migration is done
func (m *migrator) MarkMigrateDone() {
	m.done.Store(true)
}

// IsMigrateDone check if migration is done
func (m *migrator) IsMigrateDone() bool {
	return m.done.Load()
}

func createPDClient(ctx context.Context,
	pdEndpoints []string,
	conf *security.Credential,
) (pd.Client, error) {
	grpcTLSOption, err := conf.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return pd.NewClientWithContext(
		ctx, pdEndpoints, conf.PDSecurityOption(),
		pd.WithGRPCDialOptions(
			grpcTLSOption,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		))
}

// Note: we do not use etcd transaction to migrate key
// as it has the maximum operation limit in a single transaction.
// So we use a double check mechanism to make sure the migration is complete.
// 1. check and put metaVersion
// 2. campaign old owner
// 3. update keys
// 4. check metadata consistency
// 5. update metaVersion
func (m *migrator) migrate(ctx context.Context, etcdNoMetaVersion bool, oldVersion int) error {
	pdClient, err := m.createPDClientFunc(ctx,
		m.pdEndpoints, m.config.Security)
	if err != nil {
		return errors.Trace(err)
	}
	defer pdClient.Close()

	upstreamID := pdClient.GetClusterID(ctx)
	// 1.1 check metaVersion, if the metaVersion in etcd does not match
	// m.oldMetaVersion, it means that someone has migrated the metadata
	metaVersion, err := getMetaVersion(ctx, m.cli.GetEtcdClient(), m.cli.GetClusterID())
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
		_, err := m.cli.GetEtcdClient().Put(ctx, m.metaVersionKey, fmt.Sprintf("%d", oldVersion))
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
		resp, err := m.cli.GetEtcdClient().Get(ctx, oldPrefix, clientV3.WithPrefix())
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
						zap.String("value", string(v.Value)),
						zap.Error(err))
					return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
				}
				info.UpstreamID = upstreamID
				info.Namespace = model.DefaultNamespace
				// changefeed id is a part of etcd key path
				// for example:  /tidb/cdc/changefeed/info/abcd,  abcd is the changefeed
				info.ID = strings.TrimPrefix(string(v.Key), oldChangefeedPrefix+"/")
				var str string
				str, err = info.Marshal()
				if err != nil {
					log.Error("marshal changefeed failed",
						zap.Error(err))
					return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
				}
				_, err = m.cli.GetEtcdClient().Put(ctx, newKey, str)
			} else {
				_, err = m.cli.GetEtcdClient().Put(ctx, newKey, string(v.Value))
			}
			if err != nil {
				log.Error("put new meta data failed, etcd meta data migration failed",
					zap.Error(err))
				return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
			}
		}
	}
	// put upstream id
	err = m.saveUpstreamInfo(ctx)
	if err != nil {
		log.Error("save default upstream failed, "+
			"etcd meta data migration failed", zap.Error(err))
		return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
	}

	err = m.migrateGcServiceSafePoint(ctx, pdClient,
		m.config.Security, m.cli.GetGCServiceID(), m.config.GcTTL)
	if err != nil {
		log.Error("update meta version failed, etcd meta data migration failed", zap.Error(err))
		return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
	}

	// 5. update metaVersion
	_, err = m.cli.GetEtcdClient().Put(ctx, m.metaVersionKey, fmt.Sprintf("%d", m.newMetaVersion))
	if err != nil {
		log.Error("update meta version failed, etcd meta data migration failed", zap.Error(err))
		return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
	}
	log.Info("etcd data migration successful")
	cleanOldData(ctx, m.cli.GetEtcdClient())
	log.Info("clean old etcd data successful")
	return nil
}

func cleanOldData(ctx context.Context, client *etcd.Client) {
	resp, err := client.Get(ctx, "/tidb/cdc", clientV3.WithPrefix())
	if err != nil {
		log.Warn("query data from etcd failed",
			zap.Error(err))
	}
	for _, kvPair := range resp.Kvs {
		key := string(kvPair.Key)
		if shouldDelete(key) {
			value := string(kvPair.Value)
			if strings.HasPrefix(key, oldChangefeedPrefix) {
				value = maskChangefeedInfo(kvPair.Value)
			}
			// 0 is the backup version. For now, we only support version 0
			newKey := etcd.MigrateBackupKey(0, key)
			log.Info("renaming old etcd data",
				zap.String("key", key),
				zap.String("newKey", newKey),
				zap.String("value", value))
			if _, err := client.Put(ctx, newKey,
				string(kvPair.Value)); err != nil {
				log.Info("put new key failed", zap.String("key", key),
					zap.Error(err))
			}
			if _, err := client.Delete(ctx, key); err != nil {
				log.Warn("failed to delete old data",
					zap.String("key", key),
					zap.Error(err))
			}
		}
	}
}

// old key prefix that should be removed
var oldKeyPrefix = []string{
	"/tidb/cdc/changefeed/info",
	"/tidb/cdc/job",
	"/tidb/cdc/meta/ticdc-delete-etcd-key-count",
	"/tidb/cdc/owner",
	"/tidb/cdc/capture",
	"/tidb/cdc/task/workload",
	"/tidb/cdc/task/position",
	"/tidb/cdc/task/status",
}

// shouldDelete check if a key should be deleted
func shouldDelete(key string) bool {
	for _, prefix := range oldKeyPrefix {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func maskChangefeedInfo(data []byte) string {
	value := string(data)
	oldConfig := map[string]any{}
	err := json.Unmarshal(data, &oldConfig)
	if err != nil {
		log.Info("marshal oldConfig failed",
			zap.Error(err))
	}
	sinkURI, ok := oldConfig["sink-uri"]
	if ok {
		sinkURIParsed, err := url.Parse(sinkURI.(string))
		if err != nil {
			log.Error("failed to parse sink URI", zap.Error(err))
		}
		if sinkURIParsed.User != nil && sinkURIParsed.User.String() != "" {
			sinkURIParsed.User = url.UserPassword("username", "password")
		}
		if sinkURIParsed.Host != "" {
			sinkURIParsed.Host = "***"
		}
		oldConfig["sink-uri"] = sinkURIParsed.String()
		buf, err := json.Marshal(oldConfig)
		if err != nil {
			log.Info("marshal oldConfig failed",
				zap.Error(err))
		}
		value = string(buf)
	}
	return value
}

func (m *migrator) migrateGcServiceSafePoint(ctx context.Context,
	pdClient pd.Client,
	config *security.Credential,
	newGcServiceID string,
	ttl int64,
) error {
	pc, err := pdutil.NewPDAPIClient(pdClient, config)
	if err != nil {
		log.Error("create pd api client failed", zap.Error(err))
		return errors.Trace(err)
	}
	defer pc.Close()

	gcServiceSafePoints, err := pc.ListGcServiceSafePoint(ctx)
	if err != nil {
		log.Error("list gc service safepoint failed",
			zap.Error(err))
		return errors.Trace(err)
	}
	var cdcGcSafePoint *pdutil.ServiceSafePoint
	for _, item := range gcServiceSafePoints.ServiceGCSafepoints {
		if item.ServiceID == oldGcServiceID {
			cdcGcSafePoint = item
			break
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

func (m *migrator) campaignOldOwner(ctx context.Context) error {
	sess, err := concurrency.NewSession(m.cli.GetEtcdClient().Unwrap(),
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

// Migrate migrate etcd meta data
func (m *migrator) Migrate(ctx context.Context) error {
	version, err := getMetaVersion(ctx, m.cli.GetEtcdClient(), m.cli.GetClusterID())
	if err != nil {
		return errors.Trace(err)
	}

	shouldMigrate := false
	oldVersion, newVersion := 0, cdcMetaVersion

	if version == noMetaVersion {
		if m.cli.GetClusterID() != etcd.DefaultCDCClusterID {
			// not default cluster
			log.Info("not a default cdc cluster, skip migration data",
				zap.String("cluster", m.cli.GetClusterID()))
			// put upstream id
			err = m.saveUpstreamInfo(ctx)
			if err != nil {
				log.Error("save default upstream failed, "+
					"etcd meta data migration failed",
					zap.Error(err))
				return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
			}
			_, err := m.cli.GetEtcdClient().
				Put(ctx, m.metaVersionKey, fmt.Sprintf("%d", newVersion))
			if err != nil {
				log.Error("put meta version failed", zap.Error(err))
			}
			return err
		}
		shouldMigrate = true
	} else if version > newVersion {
		log.Panic("meta version in etcd is greater than the meta version in TiCDC",
			zap.Int("etcdMetaVersion", version), zap.Int("cdcMetaVersion", m.newMetaVersion))
	} else {
		oldVersion = version
		shouldMigrate = oldVersion < newVersion
	}

	if !shouldMigrate {
		return nil
	}

	m.keyPrefixes.addPair("/tidb/cdc/changefeed/info",
		etcd.DefaultClusterAndNamespacePrefix+etcd.ChangefeedInfoKey)
	m.keyPrefixes.addPair("/tidb/cdc/job",
		etcd.DefaultClusterAndNamespacePrefix+etcd.ChangefeedStatusKey)

	return m.migrate(ctx, version == noMetaVersion, oldVersion)
}

// ShouldMigrate checks if we should migrate etcd metadata
func (m *migrator) ShouldMigrate(ctx context.Context) (bool, error) {
	version, err := getMetaVersion(ctx, m.cli.GetEtcdClient(), m.cli.GetClusterID())
	if err != nil {
		return false, errors.Trace(err)
	}
	return version != cdcMetaVersion, nil
}

// WaitMetaVersionMatched checks and waits until the metaVersion in etcd
// matched to lock cdcMetaVersion
func (m *migrator) WaitMetaVersionMatched(ctx context.Context) error {
	version, err := getMetaVersion(ctx, m.cli.GetEtcdClient(), m.cli.GetClusterID())
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
			version, err := getMetaVersion(ctx, m.cli.GetEtcdClient(), m.cli.GetClusterID())
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

// saveUpstreamInfo save the default upstream info to etcd
func (m *migrator) saveUpstreamInfo(ctx context.Context) error {
	pdClient, err := m.createPDClientFunc(ctx,
		m.pdEndpoints, m.config.Security)
	if err != nil {
		return errors.Trace(err)
	}
	defer pdClient.Close()

	upstreamID := pdClient.GetClusterID(ctx)
	upstreamKey := etcd.CDCKey{
		Tp:         etcd.CDCKeyTypeUpStream,
		ClusterID:  m.cli.GetClusterID(),
		UpstreamID: upstreamID,
		Namespace:  model.DefaultNamespace,
	}
	upstreamKeyStr := upstreamKey.String()
	upstreamInfo := &model.UpstreamInfo{
		ID:            upstreamID,
		PDEndpoints:   strings.Join(m.pdEndpoints, ","),
		KeyPath:       m.config.Security.KeyPath,
		CertPath:      m.config.Security.CertPath,
		CAPath:        m.config.Security.CAPath,
		CertAllowedCN: m.config.Security.CertAllowedCN,
	}
	upstreamInfoStr, err := upstreamInfo.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.cli.GetEtcdClient().Put(ctx, upstreamKeyStr, string(upstreamInfoStr))
	return err
}

func getMetaVersion(ctx context.Context, cli *etcd.Client, clusterID string) (int, error) {
	key := etcd.CDCKey{Tp: etcd.CDCKeyTypeMetaVersion, ClusterID: clusterID}
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

// NoOpMigrator do nothing
type NoOpMigrator struct{}

// ShouldMigrate checks if we need to migrate metadata
func (f *NoOpMigrator) ShouldMigrate(_ context.Context) (bool, error) {
	return false, nil
}

// Migrate migrates the cdc metadata
func (f *NoOpMigrator) Migrate(_ context.Context) error {
	return nil
}

// WaitMetaVersionMatched wait util migration is done
func (f *NoOpMigrator) WaitMetaVersionMatched(_ context.Context) error {
	return nil
}

// MarkMigrateDone marks migration is done
func (f *NoOpMigrator) MarkMigrateDone() {
}

// IsMigrateDone check if migration is done
func (f *NoOpMigrator) IsMigrateDone() bool {
	return true
}

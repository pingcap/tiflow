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
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	etcdSessionTTL          = 10
	campaignTimeoutDuration = 1 * time.Minute
	noMetaVersion           = -1
	migrateLogsWarnDuration = 5 * time.Second
	migrationCampaignKey    = "ticdc-migration"
	oldChangefeedPrefix     = "/tidb/cdc/changefeed/info"
	oldGcServiceID          = "ticdc"
)

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
	// etcd client
	cli *etcd.CDCEtcdClient

	done atomic.Bool

	pdEndpoints []string
	config      *config.ServerConfig

	createPDClientFunc func(ctx context.Context,
		pdEndpoints []string,
		conf *security.Credential) (pd.Client, error)

	migrateGcServiceSafePointFunc func(ctx context.Context,
		pdClient pd.Client,
		config *security.Credential,
		gcServiceID string,
		ttl int64) error
}

// NewMigrator returns a cdc metadata
func NewMigrator(cli *etcd.CDCEtcdClient,
	pdEndpoints []string,
	serverConfig *config.ServerConfig,
) Migrator {
	sort.Sort(byVersion(migrationRecords))
	return &migrator{
		newMetaVersion:                migrationRecords[len(migrationRecords)-1].version,
		metaVersionKey:                etcd.DefaultClusterAndMetaPrefix + etcd.MetaVersionKey,
		cli:                           cli,
		pdEndpoints:                   pdEndpoints,
		config:                        serverConfig,
		createPDClientFunc:            createPDClient,
		migrateGcServiceSafePointFunc: migrateGcServiceSafePoint,
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

// Migrate migrate etcd meta data
func (m *migrator) Migrate(ctx context.Context) error {
	version, err := getMetaVersion(ctx, m.cli.Client, m.cli.ClusterID)
	if err != nil {
		return errors.Trace(err)
	}

	shouldMigrate := false
	oldVersion, newVersion := 0, m.newMetaVersion

	if version == noMetaVersion {
		shouldMigrate = true
	} else {
		oldVersion = version
		shouldMigrate = oldVersion < newVersion
	}

	if !shouldMigrate {
		return nil
	}
	for _, record := range migrationRecords {
		if record.version > oldVersion {
			if err := record.migrate(ctx, m); err != nil {
				log.Error("migrate failed",
					zap.Int("version", record.version),
					zap.Error(err))
				return errors.Trace(err)
			}
			// update metaVersion
			_, err = m.cli.Client.Put(ctx, m.metaVersionKey, fmt.Sprintf("%d", m.newMetaVersion))
			if err != nil {
				log.Error("update meta version failed, etcd meta data migration failed", zap.Error(err))
				return cerror.WrapError(cerror.ErrEtcdMigrateFailed, err)
			}
			log.Info("etcd data migration successful",
				zap.Int("version", record.version))
		}
	}
	return nil
}

// ShouldMigrate checks if we should migrate etcd meta data
func (m *migrator) ShouldMigrate(ctx context.Context) (bool, error) {
	version, err := getMetaVersion(ctx, m.cli.Client, m.cli.ClusterID)
	if err != nil {
		return false, errors.Trace(err)
	}
	return version != m.newMetaVersion, nil
}

// WaitMetaVersionMatched checks and waits until the metaVersion in etcd
// matched to lock cdcMetaVersion
func (m *migrator) WaitMetaVersionMatched(ctx context.Context) error {
	version, err := getMetaVersion(ctx, m.cli.Client, m.cli.ClusterID)
	if err != nil {
		return errors.Trace(err)
	}
	if version == m.newMetaVersion {
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
			version, err := getMetaVersion(ctx, m.cli.Client, m.cli.ClusterID)
			if err != nil {
				return errors.Trace(err)
			}
			if version == m.newMetaVersion {
				return nil
			}
		case <-warnLogTicker.C:
			log.Warn("meta data migrating last too long",
				zap.Duration("duration", time.Since(start)))
		}
	}
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

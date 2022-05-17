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
	cerror "github.com/pingcap/tiflow/pkg/errors"
	clientV3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/zap"
)

const (
	// cdcMetaVersion is hard code value indicate the metaVersion of TiCDC
	cdcMetaVersion       = 1
	etcdSessionTTL       = 10 * time.Second
	migrationCampaignKey = "ticdc-migration"
)

type keys map[string]string

func (k keys) addPair(old, new string) {
	k[old] = new
}

type migration struct {
	oldMetaVersion int
	newMetaVersion int
	metaVersionKey string
	// cdc onwer key in source etcd
	scrOwnerKey string
	// destination etcd client
	desCli           *Client
	keys             keys
	srcNoMetaVersion bool
}

func (m migration) migrate(ctx context.Context) error {
	// 1.campaign old owner to make sure old keys will not be updates
	if err := m.campaignOldOwner(ctx); err != nil {
		return errors.Trace(err)
	}

	// 2.campaign owner successfully, begin to migrate data
	var cmps []clientV3.Cmp
	var opsThen []clientV3.Op
	txnEmptyOpsElse := []clientV3.Op{}

	// make sure metaVersion
	if m.srcNoMetaVersion {
		cmps = append(cmps, clientV3.Compare(clientV3.CreateRevision(m.metaVersionKey), "=", 0))
	} else {
		cmps = append(cmps, clientV3.Compare(clientV3.Value(m.metaVersionKey), "=", fmt.Sprintf("%d", m.oldMetaVersion)))
	}

	for oldKey, newKey := range m.keys {
		resp, err := m.desCli.Get(ctx, oldKey, clientV3.WithPrefix())
		if err != nil {
			return errors.Trace(err)
		}
		for _, v := range resp.Kvs {
			oldKey := string(v.Key)
			// make sure newKey doesn't exist
			cmps = append(cmps, clientV3.Compare(clientV3.CreateRevision(newKey), "=", 0))
			// make sure oldKey exists
			cmps = append(cmps, clientV3.Compare(clientV3.ModRevision(oldKey), "!=", 0))
			// put newKeys on
			opsThen = append(opsThen, clientV3.OpPut(newKey, string(v.Value)))
		}
	}

	// update meataVersion
	opsThen = append(opsThen, clientV3.OpPut(m.metaVersionKey, fmt.Sprintf("%d", m.newMetaVersion)))

	txnResp, err := m.desCli.Txn(ctx, cmps, opsThen, txnEmptyOpsElse)
	if err != nil {
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if !txnResp.Succeeded {
		log.Warn("migration compare failed")
		return cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	log.Info("etcd data migration done")
	return nil
}

func (m migration) campaignOldOwner(ctx context.Context) error {
	sess, err := concurrency.NewSession(m.desCli.Unwrap(),
		concurrency.WithTTL(int(etcdSessionTTL)))
	if err != nil {
		return errors.Trace(err)
	}
	election := concurrency.NewElection(sess, m.scrOwnerKey)
	electionCtx, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	defer func() {
		_ = sess.Close()
	}()

	// TODO(dongmen): do more investigration about how to handle error here
	if err := election.Campaign(electionCtx, migrationCampaignKey); err != nil {
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
	resp, err := cli.Get(ctx, metaVersionKey)
	if err != nil {
		return errors.Trace(err)
	}
	shouldMigrate := false

	var oldVersion int
	newVersion := cdcMetaVersion
	noMetaVersionKey := false

	if len(resp.Kvs) == 0 {
		shouldMigrate = true
		noMetaVersionKey = true
	} else {
		oldVersion, err = strconv.Atoi(string(resp.Kvs[0].Value))
		if err != nil {
			return errors.Trace(err)
		}
		shouldMigrate = oldVersion < newVersion
	}

	if !shouldMigrate {
		return nil
	}

	m := migration{
		oldMetaVersion:   oldVersion,
		newMetaVersion:   cdcMetaVersion,
		metaVersionKey:   DefaultClusterAndMetaPrefix + metaVersionKey,
		scrOwnerKey:      "/ticdc/cdc/owner",
		desCli:           cli,
		keys:             make(keys),
		srcNoMetaVersion: noMetaVersionKey,
	}
	m.keys.addPair("/tidb/cdc/changefeed/info", DefaultClusterAndNamespacePrefix+changefeedInfoKey)
	m.keys.addPair("/tidb/cdc/", DefaultClusterAndNamespacePrefix+changefeedStatusKey)

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
	metaVersionKey := DefaultClusterAndMetaPrefix + metaVersionKey
	resp, err := cli.Get(ctx, metaVersionKey)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	} else {
		version, err := strconv.Atoi(string(resp.Kvs[0].Value))
		if err != nil {
			return 0, errors.Trace(err)
		}
		return version, nil
	}
}

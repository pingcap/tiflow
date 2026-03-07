// Copyright 2021 PingCAP, Inc.
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

package utils

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	tidbServerInformationPath = "/tidb/server/info"
	defaultEtcdDialTimeOut    = 3 * time.Second

	defaultGCSafePointTTL = 5 * 60
)

var (
	tidbVersionRegex       = regexp.MustCompile(`-[v]?\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
	autoGCSafePointVersion = semver.New("4.0.0")
)

func getPDDDLIDs(pCtx context.Context, cli *clientv3.Client) ([]string, error) {
	ctx, cancel := context.WithTimeout(pCtx, 10*time.Second)
	defer cancel()

	resp, err := cli.Get(ctx, tidbServerInformationPath, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	pdDDLIds := make([]string, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		items := strings.Split(string(kv.Key), "/")
		pdDDLIds[i] = items[len(items)-1]
	}
	return pdDDLIds, nil
}

// getTiDBDDLIDs gets DDL IDs from TiDB
func getTiDBDDLIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	query := "SELECT * FROM information_schema.tidb_servers_info;"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return []string{}, errors.Annotatef(err, "sql: %s", query)
	}
	return GetSpecifiedColumnValueAndClose(rows, "DDL_ID")
}

func checkSameCluster(ctx context.Context, db *sql.DB, pdAddrs []string) (bool, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdAddrs,
		DialTimeout: defaultEtcdDialTimeOut,
	})
	if err != nil {
		return false, errors.Trace(err)
	}
	tidbDDLIDs, err := getTiDBDDLIDs(ctx, db)
	if err != nil {
		return false, err
	}
	pdDDLIDs, err := getPDDDLIDs(ctx, cli)
	if err != nil {
		return false, err
	}
	sort.Strings(tidbDDLIDs)
	sort.Strings(pdDDLIDs)

	return sameStringArray(tidbDDLIDs, pdDDLIDs), nil
}

func sameStringArray(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// GetPDClientForGC is an initialization step.
func GetPDClientForGC(ctx context.Context, db *sql.DB) (pd.Client, error) {
	if ok, _ := dbutil.IsTiDB(ctx, db); ok {
		pdAddrs, err := GetPDAddrs(ctx, db)
		if err != nil {
			return nil, err
		}
		if len(pdAddrs) > 0 {
			if same, err := checkSameCluster(ctx, db, pdAddrs); err != nil {
				log.Info("[automatically GC] check whether fetched pd addr and TiDB belong to one cluster failed", zap.Strings("pd address", pdAddrs), zap.Error(err))
			} else if same {
				pdClient, err := pd.NewClientWithContext(ctx, pdAddrs, pd.SecurityOption{})
				if err != nil {
					log.Info("[automatically GC] create pd client to control GC failed", zap.Strings("pd address", pdAddrs), zap.Error(err))
					return nil, err
				}
				return pdClient, nil
			}
		}
	}
	return nil, nil
}

// GetPDAddrs gets PD address from TiDB
func GetPDAddrs(ctx context.Context, db *sql.DB) ([]string, error) {
	query := "SELECT * FROM information_schema.cluster_info where type = 'pd';"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return []string{}, errors.Annotatef(err, "sql: %s", query)
	}
	return GetSpecifiedColumnValueAndClose(rows, "STATUS_ADDRESS")
}

// GetSpecifiedColumnValueAndClose get columns' values whose name is equal to columnName and close the given rows
func GetSpecifiedColumnValueAndClose(rows *sql.Rows, columnName string) ([]string, error) {
	if rows == nil {
		return []string{}, nil
	}
	defer rows.Close()
	columnName = strings.ToUpper(columnName)
	var strs []string
	columns, _ := rows.Columns()
	addr := make([]interface{}, len(columns))
	oneRow := make([]sql.NullString, len(columns))
	fieldIndex := -1
	for i, col := range columns {
		if strings.ToUpper(col) == columnName {
			fieldIndex = i
		}
		addr[i] = &oneRow[i]
	}
	if fieldIndex == -1 {
		return strs, nil
	}
	for rows.Next() {
		err := rows.Scan(addr...)
		if err != nil {
			return strs, errors.Trace(err)
		}
		if oneRow[fieldIndex].Valid {
			strs = append(strs, oneRow[fieldIndex].String)
		}
	}
	return strs, errors.Trace(rows.Err())
}

// parse versino string to semver.Version
func parseVersion(versionStr string) (*semver.Version, error) {
	versionStr = tidbVersionRegex.FindString(versionStr)[1:]
	versionStr = strings.TrimPrefix(versionStr, "v")
	return semver.NewVersion(versionStr)
}

// TryToGetVersion gets the version of current db.
// It's OK to failed to get db version
func TryToGetVersion(ctx context.Context, db *sql.DB) *semver.Version {
	versionStr, err := dbutil.GetDBVersion(ctx, db)
	if err != nil {
		return nil
	}
	if !strings.Contains(strings.ToLower(versionStr), "tidb") {
		return nil
	}
	version, err := parseVersion(versionStr)
	if err != nil {
		// It's OK when parse version failed
		version = nil
	}
	return version
}

// StartGCSavepointUpdateService keeps GC safePoint stop moving forward.
func StartGCSavepointUpdateService(ctx context.Context, pdCli pd.Client, db *sql.DB, snapshot string) error {
	versionStr, err := selectVersion(db)
	if err != nil {
		log.Info("detect version of tidb failed")
		return nil
	}
	tidbVersion, err := parseVersion(versionStr)
	if err != nil {
		log.Info("parse version of tidb failed")
		return nil
	}
	// get latest snapshot
	snapshotTS, err := ParseSnapshotToTSO(db, snapshot)
	if tidbVersion.Compare(*autoGCSafePointVersion) > 0 {
		log.Info("tidb support auto gc safepoint", zap.Stringer("version", tidbVersion))
		if err != nil {
			return err
		}
		go updateServiceSafePoint(ctx, pdCli, snapshotTS)
	} else {
		log.Info("tidb doesn't support auto gc safepoint", zap.Stringer("version", tidbVersion))
	}
	return nil
}

func updateServiceSafePoint(ctx context.Context, pdClient pd.Client, snapshotTS uint64) {
	updateInterval := time.Duration(defaultGCSafePointTTL/2) * time.Second
	tick := time.NewTicker(updateInterval)
	DiffServiceSafePointID := fmt.Sprintf("Sync_diff_%d", time.Now().UnixNano())
	log.Info("generate dumpling gc safePoint id", zap.String("id", DiffServiceSafePointID))
	for {
		log.Debug("update PD safePoint limit with ttl",
			zap.Uint64("safePoint", snapshotTS),
			zap.Duration("updateInterval", updateInterval))
		for retryCnt := 0; retryCnt <= 10; retryCnt++ {
			_, err := pdClient.UpdateServiceGCSafePoint(ctx, DiffServiceSafePointID, defaultGCSafePointTTL, snapshotTS)
			if err == nil {
				break
			}
			log.Debug("update PD safePoint failed", zap.Error(err), zap.Int("retryTime", retryCnt))
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// ParseSnapshotToTSO parse snapshot string to TSO
func ParseSnapshotToTSO(pool *sql.DB, snapshot string) (uint64, error) {
	snapshotTS, err := strconv.ParseUint(snapshot, 10, 64)
	if err == nil {
		return snapshotTS, nil
	}
	var tso sql.NullInt64
	query := "SELECT unix_timestamp(?)"
	row := pool.QueryRow(query, snapshot)
	err = row.Scan(&tso)
	if err != nil {
		return 0, errors.Annotatef(err, "sql: %s", strings.ReplaceAll(query, "?", fmt.Sprintf(`"%s"`, snapshot)))
	}
	if !tso.Valid {
		return 0, errors.Errorf("snapshot %s format not supported. please use tso or '2006-01-02 15:04:05' format time", snapshot)
	}
	return uint64(tso.Int64*1000) << 18, nil
}

// GetSnapshot gets the snapshot
func GetSnapshot(ctx context.Context, db *sql.DB) ([]string, error) {
	query := "SHOW MASTER STATUS;"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return []string{}, errors.Annotatef(err, "sql: %s", query)
	}
	return GetSpecifiedColumnValueAndClose(rows, "Position")
}

func selectVersion(db *sql.DB) (string, error) {
	var versionInfo string
	const query = "SELECT version()"
	row := db.QueryRow(query)
	err := row.Scan(&versionInfo)
	if err != nil {
		return "", errors.Annotatef(err, "sql: %s", query)
	}
	return versionInfo, nil
}

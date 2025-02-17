// Copyright 2025 PingCAP, Inc.
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

package check

import (
	"context"
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	dbConnImpl              pmysql.IDBConnectionFactory = &pmysql.DBConnectionFactory{}
	checkIsTiDB                                         = pmysql.CheckIsTiDB
	getClusterIDBySinkURIFn                             = getClusterIDBySinkURI
)

// UpstreamDownstreamNotSame checks whether the upstream and downstream are not the same cluster.
func UpstreamDownstreamNotSame(ctx context.Context, upPD pd.Client, downSinkURI string) (bool, error) {
	upID := upPD.GetClusterID(ctx)

	downID, isTiDB, err := getClusterIDBySinkURIFn(ctx, downSinkURI)
	log.Debug("CheckNotSameUpstreamDownstream",
		zap.Uint64("upID", upID), zap.Uint64("downID", downID), zap.Bool("isTiDB", isTiDB))
	if err != nil {
		log.Error("failed to get cluster ID from sink URI",
			zap.String("downSinkURI", downSinkURI), zap.Error(err))
		return false, cerror.Trace(err)
	}

	if !isTiDB {
		return true, nil
	}

	return upID != downID, nil
}

// getClusterIDBySinkURI gets the cluster ID by the sink URI.
// Returns the cluster ID, whether it is a TiDB cluster, and an error.
func getClusterIDBySinkURI(ctx context.Context, sinkURI string) (uint64, bool, error) {
	// Create a MySQL connection by using the sink URI.
	url, err := url.Parse(sinkURI)
	if err != nil {
		return 0, true, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	if strings.ToLower(url.Scheme) != "mysql" {
		return 0, false, nil
	}
	dsnStr, err := pmysql.GenerateDSN(ctx, url, pmysql.NewConfig(), dbConnImpl.CreateTemporaryConnection)
	if err != nil {
		return 0, true, cerror.Trace(err)
	}
	db, err := dbConnImpl.CreateStandardConnection(ctx, dsnStr)
	if err != nil {
		return 0, true, cerror.Trace(err)
	}
	defer db.Close()

	// Check whether the downstream is TiDB.
	isTiDB := checkIsTiDB(ctx, db)
	if !isTiDB {
		return 0, false, nil
	}

	// Get the cluster ID from the downstream TiDB.
	row := db.QueryRowContext(ctx, "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'")
	var clusterIDStr string
	err = row.Scan(&clusterIDStr)
	if err != nil {
		// If the cluster ID is not set, it is a legacy TiDB cluster, these should be compatible with it.
		return 0, false, nil
	}
	clusterID, err := strconv.ParseUint(clusterIDStr, 10, 64)
	if err != nil {
		return 0, true, cerror.Trace(err)
	}
	return clusterID, true, nil
}

// GetGetClusterIDBySinkURIFn returns the getClusterIDBySinkURIFn function.
// It is used for testing.
func GetGetClusterIDBySinkURIFn() func(context.Context, string) (uint64, bool, error) {
	return getClusterIDBySinkURIFn
}

// SetGetClusterIDBySinkURIFnForTest sets the getClusterIDBySinkURIFn function for testing.
func SetGetClusterIDBySinkURIFnForTest(fn func(context.Context, string) (uint64, bool, error)) {
	getClusterIDBySinkURIFn = fn
}

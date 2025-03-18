// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package check

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

// Mock PD Client
type mockPDClient struct {
	pd.Client
	clusterID uint64
}

func (m *mockPDClient) GetClusterID(ctx context.Context) uint64 {
	return m.clusterID
}

// TestGetClusterIDBySinkURI
func TestGetClusterIDBySinkURI(t *testing.T) {
	// Backup and restore global variables
	oldDBConnImpl := dbConnImpl
	oldCheckIsTiDB := checkIsTiDB
	defer func() {
		dbConnImpl = oldDBConnImpl
		checkIsTiDB = oldCheckIsTiDB
	}()

	testCases := []struct {
		name          string
		sinkURI       string
		changefeedID  model.ChangeFeedID
		replicaConfig *config.ReplicaConfig
		mockDBSetup   func(sqlmock.Sqlmock)
		mockTiDBCheck bool
		wantClusterID uint64
		wantIsTiDB    bool
		wantErr       error
	}{
		{
			name:       "non mysql scheme",
			sinkURI:    "kafka://127.0.0.1:9092/topic",
			wantIsTiDB: false,
		},
		{
			name:    "invalid uri",
			sinkURI: ":invalid:",
			wantErr: cerror.ErrSinkURIInvalid.Wrap(errors.New("parse \":invalid:\": missing protocol scheme")),
		},
		{
			name:    "connect error",
			sinkURI: "mysql://user:pass@127.0.0.1:3306/db",
			mockDBSetup: func(mock sqlmock.Sqlmock) {
				dbFactory := pmysql.NewDBConnectionFactoryForTest()
				dbFactory.SetStandardConnectionFactory(
					func(ctx context.Context, dsn string) (*sql.DB, error) {
						return nil, errors.New("connect error")
					})
				dbConnImpl = dbFactory
			},
			wantErr: errors.New("connect error"),
		},
		{
			name:    "not tidb",
			sinkURI: "mysql://user:pass@127.0.0.1:3306/db",
			mockDBSetup: func(mock sqlmock.Sqlmock) {
				checkIsTiDB = func(ctx context.Context, db *sql.DB) bool { return false }
			},
			wantIsTiDB: false,
		},
		{
			name:    "tidb no cluster id",
			sinkURI: "mysql://user:pass@127.0.0.1:3306/db",
			mockDBSetup: func(mock sqlmock.Sqlmock) {
				checkIsTiDB = func(ctx context.Context, db *sql.DB) bool { return true }
				mock.ExpectQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'").
					WillReturnError(sql.ErrNoRows)
			},
			wantIsTiDB: false,
		},
		{
			name:    "success",
			sinkURI: "mysql://user:pass@127.0.0.1:3306/db",
			mockDBSetup: func(mock sqlmock.Sqlmock) {
				checkIsTiDB = func(ctx context.Context, db *sql.DB) bool { return true }
				rows := mock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("12345")
				mock.ExpectQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'").
					WillReturnRows(rows)
			},
			wantClusterID: 12345,
			wantIsTiDB:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mock database
			if tc.mockDBSetup != nil {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()

				dbFactory := pmysql.NewDBConnectionFactoryForTest()
				dbFactory.SetStandardConnectionFactory(func(ctx context.Context, dsn string) (*sql.DB, error) {
					return db, nil
				})
				dbConnImpl = dbFactory
				tc.mockDBSetup(mock)
			}
			id := model.ChangeFeedID{Namespace: "test", ID: "changefeed"}

			clusterID, isTiDB, err := getClusterIDBySinkURI(context.Background(), tc.sinkURI, id, config.GetDefaultReplicaConfig())

			if tc.wantErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantClusterID, clusterID)
				require.Equal(t, tc.wantIsTiDB, isTiDB)
			}
		})
	}
}

// TestUpstreamDownstreamNotSame
func TestUpstreamDownstreamNotSame(t *testing.T) {
	// Backup and restore global variable
	oldGetClusterID := GetGetClusterIDBySinkURIFn()
	defer func() { SetGetClusterIDBySinkURIFnForTest(oldGetClusterID) }()

	testCases := []struct {
		name         string
		upClusterID  uint64
		mockDownFunc func(context.Context, string, model.ChangeFeedID, *config.ReplicaConfig) (uint64, bool, error)
		wantResult   bool
		wantErr      error
	}{
		{
			name:        "same cluster",
			upClusterID: 123,
			mockDownFunc: func(_ context.Context, _ string, _ model.ChangeFeedID, _ *config.ReplicaConfig) (uint64, bool, error) {
				return 123, true, nil
			},
			wantResult: false,
		},
		{
			name:        "different cluster",
			upClusterID: 123,
			mockDownFunc: func(_ context.Context, _ string, _ model.ChangeFeedID, _ *config.ReplicaConfig) (uint64, bool, error) {
				return 456, true, nil
			},
			wantResult: true,
		},
		{
			name: "not tidb",
			mockDownFunc: func(_ context.Context, _ string, _ model.ChangeFeedID, _ *config.ReplicaConfig) (uint64, bool, error) {
				return 0, false, nil
			},
			wantResult: true,
		},
		{
			name: "error case",
			mockDownFunc: func(_ context.Context, _ string, _ model.ChangeFeedID, _ *config.ReplicaConfig) (uint64, bool, error) {
				return 0, false, errors.New("mock error")
			},
			wantErr: errors.New("mock error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			SetGetClusterIDBySinkURIFnForTest(tc.mockDownFunc)
			mockPD := &mockPDClient{clusterID: tc.upClusterID}
			id := model.ChangeFeedID{Namespace: "test", ID: "changefeed"}
			cfg := config.GetDefaultReplicaConfig()

			result, err := UpstreamDownstreamNotSame(context.Background(), mockPD, "any://uri", id, cfg)

			if tc.wantErr != nil {
				require.ErrorContains(t, err, tc.wantErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantResult, result)
			}
		})
	}
}

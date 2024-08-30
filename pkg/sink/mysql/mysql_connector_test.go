// Copyright 2024 PingCAP, Inc.
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

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// Test NewDBConnectorWithFactory for successful single address initialization
func TestNewDBConnectorWithFactory_SingleAddressSuccess(t *testing.T) {
	ctx := context.Background()
	sinkURI, _ := url.Parse("mysql://user:password@localhost")
	cfg := NewConfig()

	dbConnFactory := NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock, err := sqlmock.New(
			sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
			sqlmock.MonitorPingsOption(true))
		require.Nil(t, err)
		mock.ExpectPing()
		mock.ExpectClose()
		return db, nil
	})

	connector, err := NewDBConnectorWithFactory(ctx, cfg, sinkURI, dbConnFactory)
	require.NoError(t, err)
	require.NotNil(t, connector)
	require.NotNil(t, connector.CurrentDB)
	require.NoError(t, connector.CurrentDB.Ping())
	require.NoError(t, connector.CurrentDB.Close())
}

// Test NewDBConnectorWithFactory for successful multiple addresses initialization
func TestNewDBConnectorWithFactory_MultiAddressSuccess(t *testing.T) {
	ctx := context.Background()
	// Create a sinkURI which contains 3 addresses
	sinkURI, _ := url.Parse("mysql://user:password@localhost,localhost,localhost")
	cfg := NewConfig()

	dbConnFactory := &DBConnectionFactoryForTest{}
	numCallTemporary := 0
	dbConnFactory.SetTemporaryConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		numCallTemporary++
		db, err := MockTestDB()
		require.NoError(t, err)
		return db, nil
	})
	numCallStandard := 0
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		numCallStandard++
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectClose()
		return db, nil
	})

	connector, err := NewDBConnectorWithFactory(ctx, cfg, sinkURI, dbConnFactory)
	require.NoError(t, err)
	require.NotNil(t, connector)
	require.NotNil(t, connector.CurrentDB)
	require.Equal(t, numCallStandard, 1)
	require.Equal(t, numCallTemporary, 3)
	require.NoError(t, connector.CurrentDB.Close())
}

// Test NewDBConnectorWithFactory for error when generate DSNs fail
func TestNewDBConnectorWithFactory_generateDSNsFail(t *testing.T) {
	ctx := context.Background()
	sinkURI, _ := url.Parse("mysql://user:password@localhost")
	cfg := NewConfig()

	dbConnFactory := &DBConnectionFactoryForTest{}
	dbConnFactory.SetTemporaryConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		err := errors.New("")
		require.Error(t, err)
		return nil, err
	})
	numCallStandard := 0
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		numCallStandard++
		return nil, nil
	})

	connector, err := NewDBConnectorWithFactory(ctx, cfg, sinkURI, dbConnFactory)
	require.Error(t, err)
	require.Nil(t, connector)
	require.Equal(t, numCallStandard, 0)
}

// Test SwitchToAnAvailableDB when current DB is valid
func TestSwitchToAnAvailableDB_CurrentDBValid(t *testing.T) {
	ctx := context.Background()
	sinkURI, _ := url.Parse("mysql://user:password@localhost,localhost,localhost")
	cfg := NewConfig()

	dbConnFactory := NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock, err := sqlmock.New(
			sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
			sqlmock.MonitorPingsOption(true))
		require.Nil(t, err)
		mock.ExpectPing()
		mock.ExpectClose()
		return db, nil
	})

	connector, err := NewDBConnectorWithFactory(ctx, cfg, sinkURI, dbConnFactory)
	require.NoError(t, err)
	require.NotNil(t, connector)
	require.NotNil(t, connector.CurrentDB)
	dbBeforeSwitch := connector.CurrentDB

	require.NoError(t, connector.SwitchToAnAvailableDB(ctx))
	dbAfterSwitch := connector.CurrentDB
	require.Equal(t, dbBeforeSwitch, dbAfterSwitch)
	require.NoError(t, connector.CurrentDB.Close())
}

// Test SwitchToAnAvailableDB when current DB is invalid and switches to a new DB
func TestSwitchToAnAvailableDB_SwitchDB(t *testing.T) {
	ctx := context.Background()
	sinkURI, _ := url.Parse("mysql://user:password@localhost:123,localhost:456,localhost:789")
	cfg := NewConfig()

	mockDB := func() (*sql.DB, error) {
		db, mock, err := sqlmock.New(
			sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
			sqlmock.MonitorPingsOption(true))
		require.Nil(t, err)
		mock.ExpectPing()
		mock.ExpectClose()
		return db, err
	}

	var dbCandidates [3]*sql.DB
	for i := 0; i < len(dbCandidates); i++ {
		dbCandidates[i], _ = mockDB()
	}

	candidatesIndex := 0
	dbConnFactory := NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db := dbCandidates[candidatesIndex]
		candidatesIndex++
		return db, nil
	})

	connector, err := NewDBConnectorWithFactory(ctx, cfg, sinkURI, dbConnFactory)
	require.NoError(t, err)
	require.NotNil(t, connector)
	require.NotNil(t, connector.CurrentDB)

	require.NoError(t, connector.CurrentDB.Ping())
	for i := 0; i < len(dbCandidates); i++ {
		dbBeforeSwitch := connector.CurrentDB
		require.NoError(t, dbCandidates[i].Close())
		require.Error(t, dbCandidates[i].Ping())
		if i != len(dbCandidates)-1 {
			require.NoError(t, connector.SwitchToAnAvailableDB(ctx))
			require.NoError(t, connector.CurrentDB.Ping())
			dbAfterSwitch := connector.CurrentDB
			require.NotEqual(t, dbBeforeSwitch, dbAfterSwitch)
		}
	}

	require.NoError(t, connector.CurrentDB.Close())
}

// Test ConfigureDBWhenSwitch to apply configuration function
func TestConfigureDBWhenSwitch_Success(t *testing.T) {
	ctx := context.Background()
	sinkURI, _ := url.Parse("mysql://user:password@localhost,localhost")
	cfg := NewConfig()

	dbConnFactory := NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock, err := sqlmock.New(
			sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
			sqlmock.MonitorPingsOption(true))
		require.Nil(t, err)
		mock.ExpectPing()
		mock.ExpectClose()
		return db, nil
	})

	connector, err := NewDBConnectorWithFactory(ctx, cfg, sinkURI, dbConnFactory)
	require.NoError(t, err)
	require.NotNil(t, connector)
	require.NotNil(t, connector.CurrentDB)

	numCallConfigure := 0
	connector.ConfigureDBWhenSwitch(func() {
		numCallConfigure++
	}, true)

	require.NoError(t, connector.CurrentDB.Ping())
	require.NoError(t, connector.CurrentDB.Close())
	require.NoError(t, connector.SwitchToAnAvailableDB(ctx))
	require.NoError(t, connector.CurrentDB.Ping())
	require.NoError(t, connector.CurrentDB.Close())
	require.Equal(t, numCallConfigure, 2)
}

// Test ConfigureDBWhenSwitch when configuration function is nil and switches to a new DB
func TestConfigureDBWhenSwitch_NilConfigureFunction(t *testing.T) {
	ctx := context.Background()
	sinkURI, _ := url.Parse("mysql://user:password@localhost,localhost")
	cfg := NewConfig()

	dbConnFactory := NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock, err := sqlmock.New(
			sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
			sqlmock.MonitorPingsOption(true))
		require.Nil(t, err)
		mock.ExpectClose()
		return db, nil
	})

	connector, err := NewDBConnectorWithFactory(ctx, cfg, sinkURI, dbConnFactory)
	require.NoError(t, err)
	require.NotNil(t, connector)
	require.NotNil(t, connector.CurrentDB)

	connector.ConfigureDBWhenSwitch(nil, true)

	require.NoError(t, connector.CurrentDB.Close())
	require.NoError(t, connector.SwitchToAnAvailableDB(ctx))
	require.NoError(t, connector.CurrentDB.Close())
}

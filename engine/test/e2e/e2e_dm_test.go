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

package e2e_test

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/pingcap/tiflow/tests/integration_tests/util"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/pb"
)

func TestDMJob(t *testing.T) {
	ctx := context.Background()
	masterClient, err := client.NewMasterClient(ctx, []string{"127.0.0.1:10245"})
	require.NoError(t, err)

	noError := func(_ interface{}, err error) {
		require.NoError(t, err)
	}

	mysqlCfg := util.DBConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "123456",
	}
	tidbCfg := util.DBConfig{
		Host:     "127.0.0.1",
		Port:     4000,
		User:     "root",
		Password: "",
	}

	mysql, err := util.CreateDB(mysqlCfg)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, util.CloseDB(mysql))
	}()
	tidb, err := util.CreateDB(tidbCfg)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, util.CloseDB(tidb))
	}()

	// clean up
	noError(tidb.Exec("drop database if exists dm_meta"))
	noError(tidb.Exec("drop database if exists test"))
	noError(mysql.Exec("drop database if exists test"))

	// full phase
	noError(mysql.Exec("create database test"))
	noError(mysql.Exec("create table test.t1(c int primary key)"))
	noError(mysql.Exec("insert into test.t1 values(1)"))

	dmJobCfg, err := ioutil.ReadFile("./dm-job.yaml")
	require.NoError(t, err)
	// TODO: in #272, Server.jobManager is assigned after Server.leader.Store(), so even if we pass the PreRPC check,
	// Server.jobManager may not be assigned yet. We simply sleep here
	time.Sleep(time.Second)
	require.Eventually(t, func() bool {
		resp, err := masterClient.SubmitJob(ctx, &pb.SubmitJobRequest{
			Tp:     pb.JobType_DM,
			Config: dmJobCfg,
		})
		return err == nil && resp.Err == nil
	}, time.Second*5, time.Millisecond*100)

	// check full phase
	waitRow := func(where string) {
		require.Eventually(t, func() bool {
			rs, err := tidb.Query("select 1 from test.t1 where " + where)
			if err != nil {
				t.Logf("query error: %v", err)
				return false
			}
			if !rs.Next() {
				t.Log("no rows")
				return false
			}
			if rs.Next() {
				t.Log("more than one row")
				return false
			}
			return true
		}, 30*time.Second, 500*time.Millisecond)
	}
	waitRow("c = 1")

	// incremental phase
	noError(mysql.Exec("insert into test.t1 values(2)"))
	waitRow("c = 2")

	// imitate an error that can auto resume
	noError(tidb.Exec("drop table test.t1"))
	noError(mysql.Exec("insert into test.t1 values(3)"))
	time.Sleep(time.Second)
	noError(tidb.Exec("create table test.t1(c int primary key)"))
	time.Sleep(time.Second)

	// check auto resume
	waitRow("c = 3")
}

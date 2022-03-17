package e2e_test

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/pingcap/tiflow/tests/integration_tests/util"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
)

func TestDMSubtask(t *testing.T) {
	ctx := context.Background()
	masterClient, err := client.NewMasterClient(ctx, []string{"127.0.0.1:10240"})
	require.NoError(t, err)

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
	_, err = tidb.Exec("drop database if exists dmmeta")
	require.NoError(t, err)
	_, err = tidb.Exec("drop database if exists test")
	require.NoError(t, err)
	_, err = mysql.Exec("drop database if exists test")
	require.NoError(t, err)

	// full phase
	_, err = mysql.Exec("create database test")
	require.NoError(t, err)
	_, err = mysql.Exec("create table test.t1(c int primary key)")
	require.NoError(t, err)
	_, err = mysql.Exec("insert into test.t1 values(1)")
	require.NoError(t, err)

	dmSubtask, err := ioutil.ReadFile("./dm-subtask.toml")
	require.NoError(t, err)
	resp, err := masterClient.SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:     pb.JobType_DM,
		Config: dmSubtask,
	})
	require.NoError(t, err)
	require.Nil(t, resp.Err)

	// check full phase
	require.Eventually(t, func() bool {
		rs, err := tidb.Query("select 1 from test.t1 where c = 1")
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
	}, 5*time.Second, 500*time.Millisecond)

	// incremental phase
	_, err = mysql.Exec("insert into test.t1 values(2)")
	require.NoError(t, err)

	// check incremental phase
	require.Eventually(t, func() bool {
		rs, err := tidb.Query("select 1 from test.t1 where c = 2")
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
	}, 5*time.Second, 500*time.Millisecond)
}

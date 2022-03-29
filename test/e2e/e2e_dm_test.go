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
	noError(tidb.Exec("drop database if exists dmmeta"))
	noError(tidb.Exec("drop database if exists test"))
	noError(mysql.Exec("drop database if exists test"))

	// full phase
	noError(mysql.Exec("create database test"))
	noError(mysql.Exec("create table test.t1(c int primary key)"))
	noError(mysql.Exec("insert into test.t1 values(1)"))

	dmSubtask, err := ioutil.ReadFile("./dm-subtask.toml")
	require.NoError(t, err)
	resp, err := masterClient.SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:     pb.JobType_DM,
		Config: dmSubtask,
	})
	require.NoError(t, err)
	require.Nil(t, resp.Err)

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
		}, 10*time.Second, 500*time.Millisecond)
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

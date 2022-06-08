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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/tests/integration_tests/util"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/client"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

func TestDMJob(t *testing.T) {
	ctx := context.Background()
	masterClient, err := client.NewMasterClient(ctx, []string{"127.0.0.1:10245"})
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
	_, err = tidb.Exec("drop database if exists dm_meta")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		testSimpleAllModeTask(t, masterClient, mysql, tidb, "test1")
	}()
	go func() {
		defer wg.Done()
		testSimpleAllModeTask(t, masterClient, mysql, tidb, "test2")
	}()
	wg.Wait()

	// test metrics for syncer
	jobIDs := map[string]struct{}{}
	metricsURLs := []string{
		"http://127.0.0.1:11241/metrics",
		"http://127.0.0.1:11242/metrics",
		"http://127.0.0.1:11243/metrics",
	}
	re := regexp.MustCompile(`job_id="(.{36})"`)
	for _, metricsURL := range metricsURLs {
		resp, err := http.Get(metricsURL)
		require.NoError(t, err)
		content, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		matched := re.FindAllSubmatch(content, -1)
		for _, m := range matched {
			jobIDs[string(m[1])] = struct{}{}
		}
	}

	require.Equal(t, 2, len(jobIDs))
}

// testSimpleAllModeTask extracts the common logic for a DM "all" mode task,
// `db` should not contain special character.
func testSimpleAllModeTask(
	t *testing.T,
	client client.MasterClient,
	mysql, tidb *sql.DB,
	db string,
) {
	ctx := context.Background()
	noError := func(_ interface{}, err error) {
		require.NoError(t, err)
	}

	noError(tidb.Exec("drop database if exists " + db))
	noError(mysql.Exec("drop database if exists " + db))

	// full phase
	noError(mysql.Exec("create database " + db))
	noError(mysql.Exec("create table " + db + ".t1(c int primary key)"))
	noError(mysql.Exec("insert into " + db + ".t1 values(1)"))

	dmJobCfg, err := ioutil.ReadFile("./dm-job.yaml")
	require.NoError(t, err)
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte("<placeholder>"), []byte(db))
	var resp *pb.SubmitJobResponse
	require.Eventually(t, func() bool {
		resp, err = client.SubmitJob(ctx, &pb.SubmitJobRequest{
			Tp:     pb.JobType_DM,
			Config: dmJobCfg,
		})
		return err == nil && resp.Err == nil
	}, time.Second*5, time.Millisecond*100)

	// check full phase
	waitRow := func(where string) {
		require.Eventually(t, func() bool {
			rs, err := tidb.Query("select 1 from " + db + ".t1 where " + where)
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
	noError(mysql.Exec("insert into " + db + ".t1 values(2)"))
	waitRow("c = 2")

	// imitate an error that can auto resume
	noError(tidb.Exec("drop table " + db + ".t1"))
	noError(mysql.Exec("insert into " + db + ".t1 values(3)"))
	time.Sleep(time.Second)
	noError(tidb.Exec("create table " + db + ".t1(c int primary key)"))
	time.Sleep(time.Second)

	// check auto resume
	waitRow("c = 3")

	// check query status
	var args struct {
		Tasks []string
	}
	source1 := "mysql-replica-01"
	source2 := "mysql-replica-02"
	args.Tasks = []string{source1, source2}
	jsonArg, err := json.Marshal(args)
	require.NoError(t, err)
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp2, err := client.DebugJob(ctx2, &pb.DebugJobRequest{JobId: resp.JobId, Command: dmpkg.QueryStatus, JsonArg: string(jsonArg)})
	require.NoError(t, err)
	require.Nil(t, resp2.Err)
	var jobStatus dm.JobStatus
	require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &jobStatus))
	require.Equal(t, resp.JobId, jobStatus.JobMasterID)
	require.Contains(t, string(jobStatus.TaskStatus[source1].Status.Status), "totalEvents")
	require.Contains(t, jobStatus.TaskStatus[source2].Status.ErrorMsg, fmt.Sprintf("task %s for job not found", source2))
}

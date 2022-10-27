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
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	cvs "github.com/pingcap/tiflow/engine/jobmaster/cvsjob"
	"github.com/pingcap/tiflow/engine/test/e2e"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type Config struct {
	DemoAddrs   []string `json:"demo_address"`
	DemoHost    []string `json:"demo_host"`
	MasterAddrs []string `json:"master_address_list"`
	RecordNum   int64    `json:"demo_record_num"`
	JobNum      int      `json:"job_num"`
	DemoDataDir string   `json:"demo_data_dir"`
	FileNum     int      `json:"file_num"`
}

func NewConfigFromFile(file string) (*Config, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

type DemoClient struct {
	conn   *grpc.ClientConn
	client pb.DataRWServiceClient
}

func NewDemoClient(ctx context.Context, addr string) (*DemoClient, error) {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return &DemoClient{
		conn:   conn,
		client: pb.NewDataRWServiceClient(conn),
	}, err
}

func TestSubmitTest(t *testing.T) {
	configPath := os.Getenv("CONFIG")
	if configPath == "" {
		configPath = "./docker.json"
	}
	config, err := NewConfigFromFile(configPath)
	require.NoError(t, err)

	for _, demoAddr := range config.DemoAddrs {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		democlient, err := NewDemoClient(ctx, demoAddr)
		require.NoError(t, err)
		fmt.Println("connect demo " + demoAddr)

		resp, err := democlient.client.GenerateData(ctx, &pb.GenerateDataRequest{
			FileNum:   int32(config.FileNum),
			RecordNum: int32(config.RecordNum),
		})
		require.NoError(t, err)
		require.Empty(t, resp.ErrMsg)
	}

	flowControl := make(chan struct{}, 50)
	// avoid job swarming
	go func() {
		for i := 1; i <= config.JobNum; i++ {
			if i%50 == 0 {
				time.Sleep(100 * time.Millisecond)
			}
			flowControl <- struct{}{}
		}
	}()
	var wg sync.WaitGroup
	wg.Add(config.JobNum)
	for i := 1; i <= config.JobNum; i++ {
		demoAddr := config.DemoAddrs[i%len(config.DemoAddrs)]
		demoHost := config.DemoHost[i%len(config.DemoHost)]
		go func(idx int) {
			defer wg.Done()
			cfg := &cvs.Config{
				DstDir:  fmt.Sprintf(config.DemoDataDir+"/data%d", idx),
				SrcHost: demoHost,
				DstHost: demoHost,
				FileNum: config.FileNum,
			}
			testSubmitTest(t, cfg, config, demoAddr, flowControl)
		}(i)
	}
	wg.Wait()
}

// run this test after docker-compose has been up
func testSubmitTest(t *testing.T, cfg *cvs.Config, config *Config, demoAddr string, flowControl chan struct{}) {
	var (
		tenantID  = "e2e-test"
		projectID = "project-basic-test"
	)

	ctx := context.Background()
	fmt.Printf("connect demo\n")
	democlient, err := NewDemoClient(ctx, demoAddr)
	require.NoError(t, err)
	fmt.Printf("connect clients\n")

	for {
		resp, err := democlient.client.IsReady(ctx, &pb.IsReadyRequest{})
		require.NoError(t, err)
		if resp.Ready {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	configBytes, err := json.Marshal(cfg)
	require.NoError(t, err)

	<-flowControl
	fmt.Printf("test is ready\n")

	jobID, err := e2e.CreateJobViaHTTP(ctx, config.MasterAddrs[0],
		tenantID, projectID, pb.Job_CVSDemo, configBytes)
	require.NoError(t, err)

	fmt.Printf("job id %s\n", jobID)

	// continue to query
	for {
		ctx1, cancel := context.WithTimeout(ctx, 3*time.Second)
		job, err := e2e.QueryJobViaHTTP(ctx1, config.MasterAddrs[0],
			tenantID, projectID, jobID,
		)
		require.NoError(t, err)
		cancel()
		require.Equal(t, pb.Job_CVSDemo, job.Type)
		fmt.Printf("query id %s, status %d, time %s\n",
			jobID, int(job.State), time.Now().Format("2006-01-02 15:04:05"))
		if job.State == pb.Job_Finished {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Printf("job id %s checking\n", jobID)
	// check files
	demoResp, err := democlient.client.CheckDir(ctx, &pb.CheckDirRequest{
		Dir: cfg.DstDir,
	})
	require.NoError(t, err)
	require.Empty(t, demoResp.ErrMsg, demoResp.ErrFileIdx)
}

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
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/engine/client"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	cvs "github.com/pingcap/tiflow/engine/jobmaster/cvsjob"
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
	data, err := ioutil.ReadFile(file)
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		democlient, err := NewDemoClient(ctx, demoAddr)
		require.Nil(t, err)
		fmt.Println("connect demo " + demoAddr)

		resp, err := democlient.client.GenerateData(ctx, &pb.GenerateDataRequest{
			FileNum:   int32(config.FileNum),
			RecordNum: int32(config.RecordNum),
		})
		require.Nil(t, err)
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
	ctx := context.Background()
	fmt.Printf("connect demo\n")
	democlient, err := NewDemoClient(ctx, demoAddr)
	require.Nil(t, err)
	fmt.Printf("connect clients\n")
	masterclient, err := client.NewMasterClient(ctx, config.MasterAddrs)
	require.Nil(t, err)

	for {
		resp, err := democlient.client.IsReady(ctx, &pb.IsReadyRequest{})
		require.Nil(t, err)
		if resp.Ready {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	configBytes, err := json.Marshal(cfg)
	require.Nil(t, err)

	<-flowControl
	fmt.Printf("test is ready\n")

	resp, err := masterclient.SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: configBytes,
	})
	require.Nil(t, err)
	require.Nil(t, resp.Err)

	fmt.Printf("job id %s\n", resp.JobId)

	queryReq := &pb.QueryJobRequest{
		JobId: resp.JobId,
	}
	// continue to query
	for {
		ctx1, cancel := context.WithTimeout(ctx, 3*time.Second)
		queryResp, err := masterclient.QueryJob(ctx1, queryReq)
		require.NoError(t, err)
		require.Nil(t, queryResp.Err)
		require.Equal(t, queryResp.Tp, int64(framework.CvsJobMaster))
		cancel()
		fmt.Printf("query id %s, status %d, time %s\n", resp.JobId, int(queryResp.Status), time.Now().Format("2006-01-02 15:04:05"))
		if queryResp.Status == pb.QueryJobResponse_finished {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Printf("job id %s checking\n", resp.JobId)
	// check files
	demoResp, err := democlient.client.CheckDir(ctx, &pb.CheckDirRequest{
		Dir: cfg.DstDir,
	})
	require.Nil(t, err, resp.JobId)
	require.Empty(t, demoResp.ErrMsg, demoResp.ErrFileIdx)
}

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

	"github.com/hanfei1991/microcosm/client"
	cvsTask "github.com/hanfei1991/microcosm/executor/cvsTask"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type Config struct {
	DemoAddr    string   `json:"demo_address"`
	DemoHost    string   `json:"demo_host"`
	MasterAddrs []string `json:"master_address_list"`
	RecordNum   int64    `json:"demo_record_num"`
	JobNum      int      `json:"job_num"`
	DemoDataDir string   `json:"demo_data_dir"`
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

func openFileAndReadString(path string) (content []byte, err error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	return ioutil.ReadAll(fp)
}

func TestSubmitTest(t *testing.T) {
	configPath := os.Getenv("CONFIG")
	if configPath == "" {
		configPath = "./docker.json"
	}
	config := &Config{}
	configBytes, err := openFileAndReadString(configPath)
	require.Nil(t, err)
	err = json.Unmarshal(configBytes, config)
	require.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(config.JobNum)
	for i := 1; i <= config.JobNum; i++ {
		go func(idx int) {
			defer wg.Done()
			cfg := &cvsTask.Config{
				SrcDir:  config.DemoDataDir + "/datax",
				DstDir:  fmt.Sprintf(config.DemoDataDir+"/data%d", idx),
				SrcHost: config.DemoHost,
				DstHost: config.DemoHost,
			}
			testSubmitTest(t, cfg, config)
		}(i)
	}
	wg.Wait()
}

// run this test after docker-compose has been up
func testSubmitTest(t *testing.T, cfg *cvsTask.Config, config *Config) {
	ctx := context.Background()
	fmt.Printf("connect demo\n")
	democlient, err := NewDemoClient(ctx, config.DemoAddr)
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
	fmt.Printf("test is ready\n")

	configBytes, err := json.Marshal(cfg)
	require.Nil(t, err)

	resp, err := masterclient.SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: configBytes,
	})
	require.Nil(t, err)
	require.Nil(t, resp.Err)

	fmt.Printf("job id %s\n", resp.JobIdStr)

	queryReq := &pb.QueryJobRequest{
		JobId: resp.JobIdStr,
	}
	// continue to query
	for {
		ctx1, cancel := context.WithTimeout(ctx, 3*time.Second)
		queryResp, err := masterclient.QueryJob(ctx1, queryReq)
		require.Nil(t, err)
		require.Nil(t, queryResp.Err)
		require.Equal(t, queryResp.Tp, int64(lib.CvsJobMaster))
		cancel()
		fmt.Printf("query id %s, status %d, time %s\n", resp.JobIdStr, int(queryResp.Status), time.Now().Format("2006-01-02 15:04:05"))
		if queryResp.Status == pb.QueryJobResponse_finished {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Printf("job id %s checking\n", resp.JobIdStr)
	// check files
	demoResp, err := democlient.client.CheckDir(ctx, &pb.CheckDirRequest{
		Dir: cfg.DstDir,
	})
	require.Nil(t, err)
	require.Empty(t, demoResp.ErrFileName, demoResp.ErrMsg)
}

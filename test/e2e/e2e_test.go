package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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

var (
	DemoAddress             = "127.0.0.1:1234"
	MasterAddressList       = []string{"127.0.0.1:10245", "127.0.0.1:10246", "127.0.0.1:10247"}
	RecordNum         int64 = 10000
)

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
	cvsJobNum := os.Getenv("CVSJOBNUM")
	jobNum := 2
	if cvsJobNum != "" {
		var err error
		jobNum, err = strconv.Atoi(cvsJobNum)
		require.Nil(t, err)
	}
	var wg sync.WaitGroup
	wg.Add(jobNum)
	for i := 1; i <= jobNum; i++ {
		go func(idx int) {
			cfg := &cvsTask.Config{
				SrcDir:  "/data",
				DstDir:  fmt.Sprintf("/data%d", idx),
				SrcHost: "demo-server:1234",
				DstHost: "demo-server:1234",
			}
			testSubmitTest(t, cfg)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// run this test after docker-compose has been up
func testSubmitTest(t *testing.T, cfg *cvsTask.Config) {
	ctx := context.Background()
	democlient, err := NewDemoClient(ctx, DemoAddress)
	require.Nil(t, err)
	masterclient, err := client.NewMasterClient(ctx, MasterAddressList)
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

	resp, err := masterclient.SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: configBytes,
	})
	require.Nil(t, err)
	require.Nil(t, resp.Err)

	queryReq := &pb.QueryJobRequest{
		JobId: resp.JobIdStr,
	}
	// continue to query
	for {
		queryResp, err := masterclient.QueryJob(ctx, queryReq)
		require.Nil(t, err)
		require.Nil(t, queryResp.Err)
		require.Equal(t, queryResp.Tp, int64(lib.CvsJobMaster))
		if queryResp.Status == pb.QueryJobResponse_online {
			statusBytes := queryResp.JobMasterInfo.Status
			status := &lib.WorkerStatus{}
			err = json.Unmarshal(statusBytes, status)
			require.Nil(t, err)
			if status.Code == lib.WorkerStatusFinished {
				ext, err := strconv.ParseInt(string(status.ExtBytes), 10, 64)
				require.Nil(t, err, string(status.ExtBytes), string(statusBytes))
				require.Equal(t, ext, RecordNum)
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	// check files
	demoResp, err := democlient.client.CheckDir(ctx, &pb.CheckDirRequest{
		Dir: cfg.DstDir,
	})
	require.Nil(t, err)
	require.Empty(t, demoResp.ErrFileName, demoResp.ErrMsg)
}

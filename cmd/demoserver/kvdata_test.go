package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const (
	demoAddr = "127.0.0.1:1234"
	dirName  = "/tmp/df_test"
)

func TestDemoLogic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, []string{"demoserver", dirName, "1000"})
	defer os.RemoveAll(dirName)
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()
	conn, err := grpc.DialContext(ctx1, demoAddr, grpc.WithInsecure(), grpc.WithBlock())
	require.Nil(t, err)
	demoClt := pb.NewDataRWServiceClient(conn)
	// Test Ready
	require.Eventually(t, func() bool {
		resp, err := demoClt.IsReady(ctx, &pb.IsReadyRequest{})
		if err != nil || !resp.Ready {
			return false
		}
		return true
	}, 2*time.Second, 100*time.Millisecond)
	result, err := demoClt.CheckDir(ctx, &pb.CheckDirRequest{
		Dir: dirName,
	})
	require.Nil(t, err)
	require.Empty(t, result.ErrMsg)
	require.Empty(t, result.ErrFileName)
}

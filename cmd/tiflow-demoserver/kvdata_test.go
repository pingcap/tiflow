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

package main

import (
	"context"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestDemoLogic(t *testing.T) {
	demoAddress = "127.0.0.1:1234"
	demoDir = "/tmp/data"
	wtDir := "/tmp/data1"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startDataService(ctx)
	defer func() {
		os.RemoveAll(demoDir)
		os.RemoveAll(wtDir)
	}()
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()
	conn, err := grpc.DialContext(ctx1, demoAddress, grpc.WithInsecure(), grpc.WithBlock())
	require.Nil(t, err)
	demoClt := pb.NewDataRWServiceClient(conn)
	// Generate Data
	resp, err := demoClt.GenerateData(ctx, &pb.GenerateDataRequest{
		FileNum:   5,
		RecordNum: 100,
	})
	require.Nil(t, err)
	require.Empty(t, resp.ErrMsg)
	// Test Ready
	require.Eventually(t, func() bool {
		resp, err := demoClt.IsReady(ctx, &pb.IsReadyRequest{})
		if err != nil || !resp.Ready {
			return false
		}
		return true
	}, 2*time.Second, 100*time.Millisecond)

	// Test Read/Write Data
	readLineClt, err := demoClt.ReadLines(ctx, &pb.ReadLinesRequest{
		FileIdx: 0,
		LineNo:  []byte("0"),
	})
	require.Nil(t, err)
	wrClt, err := demoClt.WriteLines(ctx)
	require.Nil(t, err)
	strs := make([][]string, 5)
	for i := 0; i < 100; i++ {
		k := i % 5
		strs[k] = append(strs[k], strconv.Itoa(i))
	}
	for k := 0; k < 5; k++ {
		sort.Strings(strs[k])
	}
	for i := 0; i < 20; i++ {
		rlResp, err := readLineClt.Recv()
		require.Nil(t, err)
		require.Empty(t, rlResp.ErrMsg)
		require.Equal(t, false, rlResp.IsEof, i)
		require.Equal(t, strs[0][i], string(rlResp.Key))
		err = wrClt.Send(&pb.WriteLinesRequest{
			Dir:     wtDir,
			FileIdx: 0,
			Key:     rlResp.Key,
			Value:   rlResp.Val,
		})
		require.Nil(t, err)
	}
	closeRecv, err := wrClt.CloseAndRecv()
	require.Nil(t, err)
	require.Empty(t, closeRecv.ErrMsg)

	rlResp, err := readLineClt.Recv()
	require.Nil(t, err)
	require.Equal(t, true, rlResp.IsEof)

	readLineClt, err = demoClt.ReadLines(ctx, &pb.ReadLinesRequest{
		FileIdx: 1,
		LineNo:  []byte(strs[1][5]),
	})
	require.Nil(t, err)

	for i := 5; i < 20; i++ {
		rlResp, err := readLineClt.Recv()
		require.Nil(t, err)
		require.Empty(t, rlResp.ErrMsg)
		require.Equal(t, false, rlResp.IsEof, i)
		require.Equal(t, strs[1][i], string(rlResp.Key))
	}

	rlResp, err = readLineClt.Recv()
	require.Nil(t, err)
	require.Equal(t, true, rlResp.IsEof)

	result, err := demoClt.CheckDir(ctx, &pb.CheckDirRequest{
		Dir: demoDir,
	})
	require.Nil(t, err)
	require.Empty(t, result.ErrMsg)
}

// Copyright 2020 PingCAP, Inc.
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

package workerrpc

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pbmock"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
)

func TestGRPCClient(t *testing.T) {
	timeout := 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// get a random address for DM-worker
	addr := tempurl.Alloc()[len("http://"):]
	// NOTE: we don't wait for the gRPC connection establish now, in other words no need to wait for the DM-worker instance become online.
	rpcCli, err := NewGRPCClient(addr, security.Security{})
	require.NoError(t, err)

	// replace the underlying DM-worker client.
	workerCli := pbmock.NewMockWorkerClient(ctrl)
	rpcCli.client = workerCli

	reqs := []*Request{
		{
			Type:        CmdQueryStatus,
			QueryStatus: &pb.QueryStatusRequest{Name: "test"},
		},
		{
			Type:       CmdPurgeRelay,
			PurgeRelay: &pb.PurgeRelayRequest{Inactive: true},
		},
		{
			Type:          CmdOperateSchema,
			OperateSchema: &pb.OperateWorkerSchemaRequest{Op: pb.SchemaOp_SetSchema},
		},
		{
			Type:          CmdOperateV1Meta,
			OperateV1Meta: &pb.OperateV1MetaRequest{Op: pb.V1MetaOp_RemoveV1Meta},
		},
		{
			Type:        CmdHandleError,
			HandleError: &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace},
		},
	}

	workerCli.EXPECT().QueryStatus(gomock.Any(), reqs[0].QueryStatus)
	workerCli.EXPECT().PurgeRelay(gomock.Any(), reqs[1].PurgeRelay)
	workerCli.EXPECT().OperateSchema(gomock.Any(), reqs[2].OperateSchema)
	workerCli.EXPECT().OperateV1Meta(gomock.Any(), reqs[3].OperateV1Meta)
	workerCli.EXPECT().HandleError(gomock.Any(), reqs[4].HandleError)

	// others cmds are not supported.
	// NOTE: update the end cmd in the below `for` loop when adding new cmds.
OUTER:
	for cmd := CmdStartSubTask; cmd <= CmdHandleError; cmd++ {
		for _, req := range reqs {
			if req.Type == cmd {
				// supported cmd
				_, err = rpcCli.SendRequest(ctx, req, timeout)
				require.NoError(t, err)
				continue OUTER
			}
		}
		_, err = rpcCli.SendRequest(ctx, &Request{Type: cmd}, timeout)
		require.True(t, terror.ErrMasterGRPCInvalidReqType.Equal(err))
	}

	// got an error from the underlying RPC.
	err2 := errors.New("mock error")
	workerCli.EXPECT().QueryStatus(gomock.Any(), reqs[0].QueryStatus).Return(nil, err2)
	_, err = rpcCli.SendRequest(ctx, reqs[0], timeout)
	require.True(t, terror.ErrMasterGRPCRequestError.Equal(err))
	require.Equal(t, err2, errors.Cause(err))

	// close the cli.
	require.NoError(t, rpcCli.Close())

	// can't send request any more.
	_, err = rpcCli.SendRequest(ctx, reqs[0], timeout)
	require.True(t, terror.ErrMasterGRPCSendOnCloseConn.Equal(err))
}

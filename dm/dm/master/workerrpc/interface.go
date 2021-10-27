// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/dm/dm/pb"
)

// CmdType represents the concrete request type in Request or response type in Response.
type CmdType uint16

// CmdType values.
const (
	CmdStartSubTask CmdType = 1 + iota
	CmdOperateSubTask
	CmdUpdateSubTask

	CmdQueryStatus
	CmdQueryError
	CmdQueryTaskOperation
	CmdQueryWorkerConfig

	CmdSwitchRelayMaster
	CmdOperateRelay
	CmdPurgeRelay
	CmdUpdateRelay
	CmdMigrateRelay

	CmdFetchDDLInfo

	CmdOperateSchema

	CmdOperateV1Meta
	CmdHandleError
	CmdGetWorkerCfg
)

// Request wraps all dm-worker rpc requests.
type Request struct {
	Type CmdType

	QueryStatus *pb.QueryStatusRequest

	PurgeRelay *pb.PurgeRelayRequest

	OperateSchema *pb.OperateWorkerSchemaRequest

	OperateV1Meta *pb.OperateV1MetaRequest
	HandleError   *pb.HandleWorkerErrorRequest
	GetWorkerCfg  *pb.GetWorkerCfgRequest
}

// Response wraps all dm-worker rpc responses.
type Response struct {
	Type CmdType

	QueryStatus *pb.QueryStatusResponse

	PurgeRelay *pb.CommonWorkerResponse

	OperateSchema *pb.CommonWorkerResponse

	OperateV1Meta *pb.OperateV1MetaResponse
	HandleError   *pb.CommonWorkerResponse
	GetWorkerCfg  *pb.GetWorkerCfgResponse
}

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// SendRequest sends Request
	SendRequest(ctx context.Context, req *Request, timeout time.Duration) (*Response, error)

	// Close closes client and releases all data
	Close() error
}

// IsStreamAPI checks whether a request is streaming API based on CmdType.
func (req *Request) IsStreamAPI() bool {
	return req.Type == CmdFetchDDLInfo
}

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

package worker

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/v1workermeta"
)

// OperateV1Meta implements WorkerServer.OperateV1Meta.
func (s *Server) OperateV1Meta(ctx context.Context, req *pb.OperateV1MetaRequest) (*pb.OperateV1MetaResponse, error) {
	log.L().Info("", zap.String("request", "OperateV1Meta"), zap.Stringer("payload", req))

	switch req.Op {
	case pb.V1MetaOp_GetV1Meta:
		meta, err := v1workermeta.GetSubtasksMeta()
		if err != nil {
			// nolint:nilerr
			return &pb.OperateV1MetaResponse{
				Result: false,
				Msg:    err.Error(),
			}, nil
		}
		return &pb.OperateV1MetaResponse{
			Result: true,
			Meta:   meta,
		}, nil
	case pb.V1MetaOp_RemoveV1Meta:
		err := v1workermeta.RemoveSubtasksMeta()
		if err != nil {
			// nolint:nilerr
			return &pb.OperateV1MetaResponse{
				Result: false,
				Msg:    err.Error(),
			}, nil
		}
		return &pb.OperateV1MetaResponse{
			Result: true,
		}, nil
	default:
		return &pb.OperateV1MetaResponse{
			Result: false,
			Msg:    fmt.Sprintf("invalid op %s", req.Op.String()),
		}, nil
	}
}

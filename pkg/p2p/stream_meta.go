// Copyright 2021 PingCAP, Inc.
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

package p2p

import (
	"context"
	"encoding/json"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

const (
	gRPCMetaDataFieldName = "cdc-stream-meta"
)

//nolint:unused,deadcode
type streamMeta struct {
	// SenderID represents a unique identifier for the sender's process.
	// A unique SenderID should be used, should the sender's process crash and restart.
	SenderID NodeID `json:"sender-id"`

	// ReceiverID represents a unique identifier for the receiver's process.
	ReceiverID NodeID `json:"receiver-id"`

	// SenderAdvertisedAddr is used to provide a human-readable identifier for the sender.
	SenderAdvertisedAddr string `json:"sender-advertised-addr"`

	// Epoch is increased by one if the stream fails and is restarted, to prevent
	// potential data races caused by stale streams.
	Epoch int64 `json:"epoch"`
}

//nolint:unused,deadcode
func withStreamMeta(ctx context.Context, m *streamMeta) context.Context {
	jsonStr, err := json.Marshal(m)
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	md := metadata.Pairs(gRPCMetaDataFieldName, string(jsonStr))
	return metadata.NewOutgoingContext(ctx, md)
}

//nolint:unused,deadcode
func streamMetaFromCtx(ctx context.Context) (*streamMeta, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Panic("not an gRPC server stream context", zap.Stack("stack"))
	}
	vals := md.Get(gRPCMetaDataFieldName)
	if len(vals) != 1 {
		return nil, cerror.ErrPeerMessageIllegalMeta.GenWithStackByArgs()
	}
	jsonStr := vals[0]

	var m streamMeta
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return nil, cerror.ErrPeerMessageIllegalMeta.Wrap(err).GenWithStackByArgs()
	}

	return &m, nil
}

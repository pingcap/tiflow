// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package builder

import (
	"context"

	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/avro"
	"github.com/pingcap/tiflow/pkg/sink/codec/canal"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/craft"
	"github.com/pingcap/tiflow/pkg/sink/codec/csv"
	"github.com/pingcap/tiflow/pkg/sink/codec/maxwell"
	"github.com/pingcap/tiflow/pkg/sink/codec/open"
	"github.com/pingcap/tiflow/pkg/sink/codec/simple"
)

// NewRowEventEncoderBuilder returns an RowEventEncoderBuilder
func NewRowEventEncoderBuilder(
	ctx context.Context, cfg *common.Config,
) (codec.RowEventEncoderBuilder, error) {
	switch cfg.Protocol {
	case config.ProtocolDefault, config.ProtocolOpen:
		return open.NewBatchEncoderBuilder(ctx, cfg)
	case config.ProtocolCanal:
		return canal.NewBatchEncoderBuilder(cfg), nil
	case config.ProtocolAvro:
		return avro.NewBatchEncoderBuilder(ctx, cfg)
	case config.ProtocolMaxwell:
		return maxwell.NewBatchEncoderBuilder(cfg), nil
	case config.ProtocolCanalJSON:
		return canal.NewJSONRowEventEncoderBuilder(ctx, cfg)
	case config.ProtocolCraft:
		return craft.NewBatchEncoderBuilder(cfg), nil
	case config.ProtocolSimple:
		return simple.NewBuilder(ctx, cfg)
	default:
		return nil, cerror.ErrSinkUnknownProtocol.GenWithStackByArgs(cfg.Protocol)
	}
}

// NewTxnEventEncoderBuilder returns an TxnEventEncoderBuilder.
func NewTxnEventEncoderBuilder(
	c *common.Config,
) (codec.TxnEventEncoderBuilder, error) {
	switch c.Protocol {
	case config.ProtocolCsv:
		return csv.NewTxnEventEncoderBuilder(c), nil
	case config.ProtocolCanalJSON:
		return canal.NewJSONTxnEventEncoderBuilder(c), nil
	default:
		return nil, cerror.ErrSinkUnknownProtocol.GenWithStackByArgs(c.Protocol)
	}
}

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

	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/avro"
	"github.com/pingcap/tiflow/cdc/sink/codec/canal"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sink/codec/craft"
	"github.com/pingcap/tiflow/cdc/sink/codec/csv"
	"github.com/pingcap/tiflow/cdc/sink/codec/maxwell"
	"github.com/pingcap/tiflow/cdc/sink/codec/open"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// NewEventBatchEncoderBuilder returns an EncoderBuilder
func NewEventBatchEncoderBuilder(ctx context.Context, c *common.Config) (codec.EncoderBuilder, error) {
	switch c.Protocol {
	case config.ProtocolDefault, config.ProtocolOpen:
		return open.NewBatchEncoderBuilder(c), nil
	case config.ProtocolCanal:
		return canal.NewBatchEncoderBuilder(c), nil
	case config.ProtocolAvro:
		return avro.NewBatchEncoderBuilder(ctx, c)
	case config.ProtocolMaxwell:
		return maxwell.NewBatchEncoderBuilder(c), nil
	case config.ProtocolCanalJSON:
		return canal.NewJSONBatchEncoderBuilder(c), nil
	case config.ProtocolCraft:
		return craft.NewBatchEncoderBuilder(c), nil
	case config.ProtocolCsv:
		return csv.NewBatchEncoderBuilder(c), nil
	default:
		return nil, cerror.ErrSinkUnknownProtocol.GenWithStackByArgs(c.Protocol)
	}
}

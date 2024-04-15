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

package csv

import (
	"context"
	"io"

	"github.com/pingcap/errors"
	lconfig "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

const defaultIOConcurrency = 1

type batchDecoder struct {
	codecConfig *common.Config
	parser      *mydump.CSVParser
	data        []byte
	msg         *csvMessage
	tableInfo   *model.TableInfo
	closed      bool
}

// NewBatchDecoder creates a new BatchDecoder
func NewBatchDecoder(ctx context.Context,
	codecConfig *common.Config,
	tableInfo *model.TableInfo,
	value []byte,
) (codec.RowEventDecoder, error) {
	var backslashEscape bool

	// if quote is not set in config, we should unespace backslash
	// when parsing csv columns.
	if len(codecConfig.Quote) == 0 {
		backslashEscape = true
	}
	cfg := &lconfig.CSVConfig{
		Separator:       codecConfig.Delimiter,
		Delimiter:       codecConfig.Quote,
		Terminator:      codecConfig.Terminator,
		Null:            []string{codecConfig.NullString},
		BackslashEscape: backslashEscape,
	}
	csvParser, err := mydump.NewCSVParser(ctx, cfg,
		mydump.NewStringReader(string(value)),
		int64(lconfig.ReadBlockSize),
		worker.NewPool(ctx, defaultIOConcurrency, "io"), false, nil)
	if err != nil {
		return nil, err
	}
	return &batchDecoder{
		codecConfig: codecConfig,
		tableInfo:   tableInfo,
		data:        value,
		msg:         newCSVMessage(codecConfig),
		parser:      csvParser,
	}, nil
}

// AddKeyValue implements the RowEventDecoder interface.
func (b *batchDecoder) AddKeyValue(_, _ []byte) error {
	return nil
}

// HasNext implements the RowEventDecoder interface.
func (b *batchDecoder) HasNext() (model.MessageType, bool, error) {
	err := b.parser.ReadRow()
	if err != nil {
		b.closed = true
		if errors.Cause(err) == io.EOF {
			return model.MessageTypeUnknown, false, nil
		}
		return model.MessageTypeUnknown, false, err
	}

	row := b.parser.LastRow()
	if err = b.msg.decode(row.Row); err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	return model.MessageTypeRow, true, nil
}

// NextResolvedEvent implements the RowEventDecoder interface.
func (b *batchDecoder) NextResolvedEvent() (uint64, error) {
	return 0, nil
}

// NextRowChangedEvent implements the RowEventDecoder interface.
func (b *batchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.closed {
		return nil, cerror.WrapError(cerror.ErrCSVDecodeFailed, errors.New("no csv row can be found"))
	}

	e, err := csvMsg2RowChangedEvent(b.codecConfig, b.msg, b.tableInfo.Columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return e, nil
}

// NextDDLEvent implements the RowEventDecoder interface.
func (b *batchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	return nil, nil
}

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

	"github.com/pingcap/errors"
	lconfig "github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const defaultIOConcurrency = 1

type batchDecoder struct {
	csvConfig *config.CSVConfig
	parser    *mydump.CSVParser
	data      []byte
	msg       *csvMessage
	closed    bool
}

// NewBatchDecoder creates a new BatchDecoder
func NewBatchDecoder(ctx context.Context, csvConfig *config.CSVConfig, value []byte) (codec.EventBatchDecoder, error) {
	var backslashEscape bool

	// if quote is not set in config, we should unespace backslash
	// when parsing csv columns.
	if len(csvConfig.Quote) == 0 {
		backslashEscape = true
	}
	cfg := &lconfig.CSVConfig{
		Separator:       csvConfig.Delimiter,
		Delimiter:       csvConfig.Quote,
		Terminator:      csvConfig.Terminator,
		Null:            csvConfig.NullString,
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
		csvConfig: csvConfig,
		data:      value,
		msg:       newCSVMessage(csvConfig),
		parser:    csvParser,
	}, nil
}

// HasNext implements the EventBatchDecoder interface.
func (b *batchDecoder) HasNext() (model.MessageType, bool, error) {
	err := b.parser.ReadRow()
	if err != nil {
		b.closed = true
		return model.MessageTypeUnknown, false, err
	}

	row := b.parser.LastRow()
	if err = b.msg.decode(row.Row); err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	return model.MessageTypeRow, true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface.
func (b *batchDecoder) NextResolvedEvent() (uint64, error) {
	return 0, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface.
func (b *batchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.closed {
		return nil, cerror.WrapError(cerror.ErrCSVDecodeFailed, errors.New("no csv row can be found"))
	}
	return csvMsg2RowChangedEvent(b.msg), nil
}

// NextDDLEvent implements the EventBatchDecoder interface.
func (b *batchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	return nil, nil
}

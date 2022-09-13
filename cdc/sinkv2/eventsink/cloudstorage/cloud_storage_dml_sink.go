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
package cloudstorage

import (
	"context"
	"net/url"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	mqutil "github.com/pingcap/tiflow/cdc/sinkv2/util/mq"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*sink)(nil)

// sink is the cloud storage sink.
// It will send the events to cloud storage systems.
type sink struct {
	id model.ChangeFeedID
	// storage   storage.ExternalStorage
	msgChan        *chann.Chann[eventFragment]
	encoderWorkers []*encoderWorker
	writer         *dmlWriter

	tableSeqMap map[*model.TableName]uint64
}

// New creates a cloud storage sink.
func NewCloudStorageSink(ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan error,
) (*sink, error) {
	s := &sink{}
	storageURL := strings.Split(sinkURI.String(), "?")[0]
	bs, err := storage.ParseBackend(storageURL, &storage.BackendOptions{})
	if err != nil {
		return nil, err
	}
	storage, err := storage.New(ctx, bs, nil)
	if err != nil {
		return nil, err
	}

	protocol, err := mqutil.GetProtocol(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := mqutil.GetEncoderConfig(sinkURI, protocol, replicaConfig, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}

	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
	}
	encoder := encoderBuilder.Build()
	writer := newDMLWriter(ctx, storage, 8)

	s.id = changefeedID
	s.msgChan = chann.New[eventFragment]()
	for i := 0; i < 5; i++ {
		w := newWorker(i+1, changefeedID, encoder, writer, errCh)
		w.run(ctx, s.msgChan)
		s.encoderWorkers = append(s.encoderWorkers, w)
	}

	return s, nil
}

// WriteEvents write events
func (s *sink) WriteEvents(txns ...*eventsink.CallbackableEvent[*model.SingleTableTxn]) error {
	var tableName *model.TableName
	for _, txn := range txns {
		tableName = txn.Event.Table
		s.tableSeqMap[tableName]++
		s.msgChan.In() <- eventFragment{
			seqNumber: int64(s.tableSeqMap[tableName]),
			tableName: tableName,
			event:     txn,
		}
	}

	// s.writer.setLastSeqForTable(tableName, int64(s.tableSeqMap[tableName]))
	s.msgChan.In() <- eventFragment{
		tableName: tableName,
		seqNumber: int64(s.tableSeqMap[tableName]),
	}
	return nil
}

func (s *sink) Close() error {
	for _, w := range s.encoderWorkers {
		w.stop()
	}
	s.writer.stop()
	return nil
}

type eventFragment struct {
	seqNumber   int64
	tableName   *model.TableName
	event       *eventsink.CallbackableEvent[*model.SingleTableTxn]
	encodedMsgs []*common.Message
}

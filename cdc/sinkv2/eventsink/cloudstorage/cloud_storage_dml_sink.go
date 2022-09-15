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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/util"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	pcloudstorage "github.com/pingcap/tiflow/pkg/sink/cloudstorage"

	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	defaultEncodingConcurrency = 8
	defaultMaxMessageBytes     = 1073741824 //1GB
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*sink)(nil)

// sink is the cloud storage sink.
// It will send the events to cloud storage systems.
type sink struct {
	id model.ChangeFeedID
	// storage   storage.ExternalStorage
	msgChan         *chann.Chann[eventFragment]
	encodingWorkers []*encodingWorker
	writer          *dmlWriter

	tableSeqMap map[*model.TableName]uint64
}

// New creates a cloud storage sink.
func NewCloudStorageSink(ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan error,
) (*sink, error) {
	s := &sink{}
	cfg := pcloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, replicaConfig)
	if err != nil {
		return nil, err
	}

	bs, err := storage.ParseBackend(sinkURI.String(), &storage.BackendOptions{})
	if err != nil {
		return nil, err
	}

	storage, err := storage.New(ctx, bs, nil)
	if err != nil {
		return nil, err
	}

	protocol, err := util.GetProtocol(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ext := util.GetFileExtension(protocol)
	encoderConfig, err := util.GetEncoderConfig(sinkURI, protocol, replicaConfig, defaultMaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCloudStorageInvalidConfig, err)
	}

	s.writer = newDMLWriter(ctx, changefeedID, storage, cfg.WorkerCount, ext, errCh)
	s.id = changefeedID
	s.msgChan = chann.New[eventFragment]()
	for i := 0; i < defaultEncodingConcurrency; i++ {
		encoder := encoderBuilder.Build()
		w := newEncodingWorker(i+1, changefeedID, encoder, s.writer, errCh)
		w.run(ctx, s.msgChan)
		s.encodingWorkers = append(s.encodingWorkers, w)
	}

	return s, nil
}

// WriteEvents write events
func (s *sink) WriteEvents(txns ...*eventsink.CallbackableEvent[*model.SingleTableTxn]) error {
	var tableName *model.TableName
	var tableVersion uint64
	for _, txn := range txns {
		tableName = txn.Event.Table
		tableVersion = txn.Event.TableVersion
		s.tableSeqMap[tableName]++
		s.msgChan.In() <- eventFragment{
			seqNumber:    s.tableSeqMap[tableName],
			tableName:    tableName,
			tableVersion: tableVersion,
			event:        txn,
		}
	}

	s.msgChan.In() <- eventFragment{
		tableName:    tableName,
		tableVersion: tableVersion,
		seqNumber:    s.tableSeqMap[tableName],
	}
	return nil
}

func (s *sink) Close() error {
	s.msgChan.Close()
	for range s.msgChan.Out() {
		// drain the msgChan
	}
	for _, w := range s.encodingWorkers {
		w.stop()
	}
	s.writer.stop()
	return nil
}

type eventFragment struct {
	seqNumber    uint64
	tableVersion uint64
	tableName    *model.TableName
	event        *eventsink.TxnCallbackableEvent
	encodedMsgs  []*common.Message
}

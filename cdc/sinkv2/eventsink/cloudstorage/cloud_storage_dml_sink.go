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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	rcommon "github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/util"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

const (
	defaultEncodingConcurrency = 8
	defaultMaxMessageBytes     = 1073741824 // 1GB
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*sink)(nil)

// versionedTable is used to wrap TableName with a version
type versionedTable struct {
	model.TableName
	TableVersion uint64
}

// eventFragment is used to attach a sequence number to TxnCallbackableEvent.
// The sequence number is mainly useful for TxnCallbackableEvent defragmentation.
// e.g. TxnCallbackableEvent 1~5 are dispatched to a group of encoding workers, but the
// encoding completion time varies. Let's say the final completion sequence are 1,3,2,5,4,
// we can use the sequence numbers to do defragmentation so that the events can arrive
// at dmlWorker sequentially.
type eventFragment struct {
	// event sequence number
	seqNumber    uint64
	tableVersion uint64
	tableName    model.TableName
	event        *eventsink.TxnCallbackableEvent
	// encodedMsgs denote the encoded messages after the event is handled in encodingWorker.
	encodedMsgs []*common.Message
}

func eventFragmentLess(e1, e2 eventFragment) bool {
	return e1.seqNumber < e2.seqNumber
}

// sink is the cloud storage sink.
// It will send the events to cloud storage systems.
type sink struct {
	// msgChan is a unbounded channel to hold eventFragment.
	msgChan *chann.Chann[eventFragment]
	// encodingWorkers defines a group of workers for encoding events.
	encodingWorkers []*encodingWorker
	// writer is a dmlWriter which manages a group of dmlWorkers and
	// sends encoded messages to individual dmlWorkers.
	writer *dmlWriter
	// tableSeqMap maintains a <versionTable, sequenceNumber> mapping.
	tableSeqMap map[versionedTable]uint64
	mu          sync.Mutex
}

// NewCloudStorageSink creates a cloud storage sink.
func NewCloudStorageSink(ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan error,
) (*sink, error) {
	s := &sink{}
	// create cloud storage config and then apply the params of sinkURI to it.
	cfg := cloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, replicaConfig)
	if err != nil {
		return nil, err
	}

	// parse backend storage from sinkURI.
	bs, err := storage.ParseBackend(sinkURI.String(), nil)
	if err != nil {
		return nil, err
	}

	// create an external storage.
	storage, err := storage.New(ctx, bs, &storage.ExternalStorageOptions{
		SendCredentials: false,
		S3Retryer:       rcommon.DefaultS3Retryer(),
	})
	if err != nil {
		return nil, err
	}

	// fetch protocol from replicaConfig defined by changefeed config file.
	protocol, err := util.GetProtocol(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// get cloud storage file extension according to the specific protocol.
	ext := util.GetFileExtension(protocol)
	encoderConfig, err := util.GetEncoderConfig(sinkURI, protocol, replicaConfig, defaultMaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCloudStorageInvalidConfig, err)
	}

	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	s.writer = newDMLWriter(ctx, changefeedID, storage, cfg, ext, errCh)
	s.msgChan = chann.New[eventFragment]()
	s.tableSeqMap = make(map[versionedTable]uint64)

	// create a group of encoding workers.
	for i := 0; i < defaultEncodingConcurrency; i++ {
		encoder := encoderBuilder.Build()
		w := newEncodingWorker(i+1, changefeedID, encoder, s.writer, errCh)
		w.run(ctx, s.msgChan)
		s.encodingWorkers = append(s.encodingWorkers, w)
	}

	return s, nil
}

// WriteEvents write events to cloud storage sink.
func (s *sink) WriteEvents(txns ...*eventsink.CallbackableEvent[*model.SingleTableTxn]) error {
	var tbl versionedTable
	var seq uint64

	for _, txn := range txns {
		tbl = versionedTable{
			TableName:    *txn.Event.Table,
			TableVersion: txn.Event.TableVersion,
		}

		s.mu.Lock()
		s.tableSeqMap[tbl]++
		seq = s.tableSeqMap[tbl]
		s.mu.Unlock()
		// emit a TxnCallbackableEvent encoupled with a sequence number starting from one.
		s.msgChan.In() <- eventFragment{
			seqNumber:    seq,
			tableName:    tbl.TableName,
			tableVersion: tbl.TableVersion,
			event:        txn,
		}
	}

	return nil
}

// Close closes the cloud storage sink.
func (s *sink) Close() error {
	s.msgChan.Close()
	for range s.msgChan.Out() {
		// drain the msgChan
	}
	for _, w := range s.encodingWorkers {
		w.close()
	}
	s.writer.close()
	return nil
}

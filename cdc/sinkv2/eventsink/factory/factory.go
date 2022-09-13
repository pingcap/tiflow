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
// limitations under the License

package factory

import (
	"context"
	"strings"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/blackhole"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/cloudstorage"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/txn"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

// SinkFactory is the factory of sink.
// It is responsible for creating sink and closing it.
// Because there is no way to convert the eventsink.EventSink[*model.RowChangedEvent]
// to eventsink.EventSink[eventsink.TableEvent].
// So we have to use this factory to create and store the sink.
type SinkFactory struct {
	sinkType sink.Type
	rowSink  eventsink.EventSink[*model.RowChangedEvent]
	txnSink  eventsink.EventSink[*model.SingleTableTxn]
}

// New creates a new SinkFactory by schema.
func New(ctx context.Context,
	sinkURIStr string,
	cfg *config.ReplicaConfig,
	errCh chan error,
) (*SinkFactory, error) {
	sinkURI, err := config.GetSinkURIAndAdjustConfigWithSinkURI(sinkURIStr, cfg)
	if err != nil {
		return nil, err
	}

	s := &SinkFactory{}
	schema := strings.ToLower(sinkURI.Scheme)
	switch schema {
	case sink.MySQLSchema, sink.MySQLSSLSchema, sink.TiDBSchema, sink.TiDBSSLSchema:
		txnSink, err := txn.NewMySQLSink(ctx, sinkURI, cfg, errCh, txn.DefaultConflictDetectorSlots)
		if err != nil {
			return nil, err
		}
		s.txnSink = txnSink
		s.sinkType = sink.TxnSink
	case sink.KafkaSchema, sink.KafkaSSLSchema:
		mqs, err := mq.NewKafkaDMLSink(ctx, sinkURI, cfg, errCh,
			kafka.NewSaramaAdminClient, dmlproducer.NewKafkaDMLProducer)
		if err != nil {
			return nil, err
		}
		s.rowSink = mqs
		s.sinkType = sink.RowSink
	case sink.S3Schema, sink.NFSSchema, sink.LocalSchema:
		storageSink, err := cloudstorage.NewCloudStorageSink(ctx, sinkURI, cfg, errCh)
		if err != nil {
			return nil, err
		}
		s.txnSink = storageSink
		s.sinkType = sink.TxnSink
	case sink.BlackHoleSchema:
		bs := blackhole.New()
		s.rowSink = bs
		s.sinkType = sink.RowSink
	default:
		return nil,
			cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", schema)
	}

	return s, nil
}

// CreateTableSink creates a TableSink by schema.
func (s *SinkFactory) CreateTableSink(changefeedID model.ChangeFeedID, tableID model.TableID, totalRowsCounter prometheus.Counter) tablesink.TableSink {
	switch s.sinkType {
	case sink.RowSink:
		// We have to indicate the type here, otherwise it can not be compiled.
		return tablesink.New[*model.RowChangedEvent](changefeedID, tableID,
			s.rowSink, &eventsink.RowChangeEventAppender{}, totalRowsCounter)
	case sink.TxnSink:
		return tablesink.New[*model.SingleTableTxn](changefeedID, tableID,
			s.txnSink, &eventsink.TxnEventAppender{}, totalRowsCounter)
	default:
		panic("unknown sink type")
	}
}

// Close closes the sink.
func (s *SinkFactory) Close() error {
	switch s.sinkType {
	case sink.RowSink:
		return s.rowSink.Close()
	case sink.TxnSink:
		return s.txnSink.Close()
	default:
		panic("unknown sink type")
	}
}

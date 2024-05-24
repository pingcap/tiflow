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
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

// Category is for different DML sink categories.
type Category = int

const (
	// CategoryTxn is for Txn sink.
	CategoryTxn Category = 1
	// CategoryMQ is for MQ sink.
	CategoryMQ = 2
	// CategoryCloudStorage is for CloudStorage sink.
	CategoryCloudStorage = 3
	// CategoryBlackhole is for Blackhole sink.
	CategoryBlackhole = 4
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
	category Category
}

// New creates a new SinkFactory by schema.
func New(ctx context.Context,
	sinkURIStr string,
	cfg *config.ReplicaConfig,
	errCh chan error,
	pdClock pdutil.Clock,
) (*SinkFactory, error) {
	sinkURI, err := config.GetSinkURIAndAdjustConfigWithSinkURI(sinkURIStr, cfg)
	if err != nil {
		return nil, err
	}

	s := &SinkFactory{}
	scheme := strings.ToLower(sinkURI.Scheme)
	switch scheme {
	case sink.MySQLScheme, sink.MySQLSSLScheme, sink.TiDBScheme, sink.TiDBSSLScheme:
		txnSink, err := txn.NewMySQLSink(ctx, sinkURI, cfg, errCh, txn.DefaultConflictDetectorSlots)
		if err != nil {
			return nil, err
		}
		s.txnSink = txnSink
		s.sinkType = sink.TxnSink
		s.category = CategoryTxn
	case sink.KafkaScheme, sink.KafkaSSLScheme:
		mqs, err := mq.NewKafkaDMLSink(ctx, sinkURI, cfg, errCh,
			kafka.NewSaramaAdminClient, dmlproducer.NewKafkaDMLProducer)
		if err != nil {
			return nil, err
		}
		s.txnSink = mqs
		s.sinkType = sink.TxnSink
		s.category = CategoryMQ
	case sink.S3Scheme, sink.FileScheme, sink.GCSScheme, sink.GSScheme, sink.AzblobScheme, sink.AzureScheme, sink.CloudStorageNoopScheme:
		storageSink, err := cloudstorage.NewCloudStorageSink(ctx, pdClock, sinkURI, cfg, errCh)
		if err != nil {
			return nil, err
		}
		s.txnSink = storageSink
		s.sinkType = sink.TxnSink
		s.category = CategoryCloudStorage
	case sink.BlackHoleScheme:
		bs := blackhole.New()
		s.rowSink = bs
		s.sinkType = sink.RowSink
		s.category = CategoryBlackhole
	default:
		return nil,
			cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", scheme)
	}

	return s, nil
}

// CreateTableSink creates a TableSink by schema.
func (s *SinkFactory) CreateTableSink(
	changefeedID model.ChangeFeedID,
	tableID model.TableID, startTs model.Ts,
	pdClock pdutil.Clock,
	totalRowsCounter prometheus.Counter,
	flushLagDuration prometheus.Observer,
) tablesink.TableSink {
	switch s.sinkType {
	case sink.RowSink:
		// We have to indicate the type here, otherwise it can not be compiled.
		return tablesink.New[*model.RowChangedEvent](changefeedID, tableID, startTs,
			s.rowSink, &eventsink.RowChangeEventAppender{}, pdClock, totalRowsCounter, flushLagDuration)
	case sink.TxnSink:
		return tablesink.New[*model.SingleTableTxn](changefeedID, tableID, startTs,
			s.txnSink, &eventsink.TxnEventAppender{TableSinkStartTs: startTs}, pdClock, totalRowsCounter, flushLagDuration)
	default:
		panic("unknown sink type")
	}
}

// CreateTableSinkForConsumer creates a TableSink by schema for consumer.
// The difference between CreateTableSink and CreateTableSinkForConsumer is that
// CreateTableSinkForConsumer will not create a new sink for each table.
// NOTICE: This only used for the consumer. Please do not use it in the processor.
func (s *SinkFactory) CreateTableSinkForConsumer(
	changefeedID model.ChangeFeedID,
	tableID model.TableID, startTs model.Ts,
) tablesink.TableSink {
	switch s.sinkType {
	case sink.RowSink:
		// We have to indicate the type here, otherwise it can not be compiled.
		return tablesink.New[*model.RowChangedEvent](changefeedID, tableID, startTs,
			s.rowSink, &eventsink.RowChangeEventAppender{}, pdutil.NewClock4Test(),
			prometheus.NewCounter(prometheus.CounterOpts{}),
			prometheus.NewHistogram(prometheus.HistogramOpts{}))
	case sink.TxnSink:
		return tablesink.New[*model.SingleTableTxn](changefeedID, tableID, startTs, s.txnSink,
			// IgnoreStartTs is true because the consumer can
			// **not** get the start ts of the row changed event.
			&eventsink.TxnEventAppender{TableSinkStartTs: startTs, IgnoreStartTs: true},
			pdutil.NewClock4Test(),
			prometheus.NewCounter(prometheus.CounterOpts{}),
			prometheus.NewHistogram(prometheus.HistogramOpts{}))
	default:
		panic("unknown sink type")
	}
}

// Close closes the sink.
func (s *SinkFactory) Close() {
	switch s.sinkType {
	case sink.RowSink:
		s.rowSink.Close()
	case sink.TxnSink:
		s.txnSink.Close()
	default:
		panic("unknown sink type")
	}
}

// Category returns category of s.
func (s *SinkFactory) Category() Category {
	if s.category == 0 {
		panic("should never happen")
	}
	return s.category
}

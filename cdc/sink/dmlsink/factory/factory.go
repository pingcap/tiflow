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
	"net/url"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/blackhole"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/cloudstorage"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/txn"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	v2 "github.com/pingcap/tiflow/pkg/sink/kafka/v2"
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
	rowSink  dmlsink.EventSink[*model.RowChangedEvent]
	txnSink  dmlsink.EventSink[*model.SingleTableTxn]
	category Category
}

// New creates a new SinkFactory by schema.
func New(
	ctx context.Context,
	sinkURIStr string,
	cfg *config.ReplicaConfig,
	errCh chan error,
) (*SinkFactory, error) {
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	s := &SinkFactory{}
	schema := strings.ToLower(sinkURI.Scheme)
	switch schema {
	case sink.MySQLScheme, sink.MySQLSSLScheme, sink.TiDBScheme, sink.TiDBSSLScheme:
		txnSink, err := txn.NewMySQLSink(ctx, sinkURI, cfg, errCh, txn.DefaultConflictDetectorSlots)
		if err != nil {
			return nil, err
		}
		s.txnSink = txnSink
		s.category = CategoryTxn
	case sink.KafkaScheme, sink.KafkaSSLScheme:
		factoryCreator := kafka.NewSaramaFactory
		if cfg.Sink.EnableKafkaSinkV2 {
			factoryCreator = v2.NewFactory
		}
		mqs, err := mq.NewKafkaDMLSink(ctx, sinkURI, cfg, errCh,
			factoryCreator, dmlproducer.NewKafkaDMLProducer)
		if err != nil {
			return nil, err
		}
		s.txnSink = mqs
		s.category = CategoryMQ
	case sink.S3Scheme, sink.FileScheme, sink.GCSScheme, sink.GSScheme, sink.AzblobScheme, sink.AzureScheme, sink.CloudStorageNoopScheme:
		storageSink, err := cloudstorage.NewDMLSink(ctx, sinkURI, cfg, errCh)
		if err != nil {
			return nil, err
		}
		s.txnSink = storageSink
		s.category = CategoryCloudStorage
	case sink.BlackHoleScheme:
		bs := blackhole.NewDMLSink()
		s.rowSink = bs
		s.category = CategoryBlackhole
	default:
		return nil,
			cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", schema)
	}

	return s, nil
}

// CreateTableSink creates a TableSink by schema.
func (s *SinkFactory) CreateTableSink(
	changefeedID model.ChangeFeedID,
	span tablepb.Span, startTs model.Ts,
	totalRowsCounter prometheus.Counter,
) tablesink.TableSink {
	if s.txnSink != nil {
		return tablesink.New(changefeedID, span, startTs, s.txnSink,
			&dmlsink.TxnEventAppender{TableSinkStartTs: startTs}, totalRowsCounter)
	}

	return tablesink.New(changefeedID, span, startTs, s.rowSink,
		&dmlsink.RowChangeEventAppender{}, totalRowsCounter)
}

// CreateTableSinkForConsumer creates a TableSink by schema for consumer.
// The difference between CreateTableSink and CreateTableSinkForConsumer is that
// CreateTableSinkForConsumer will not create a new sink for each table.
// NOTICE: This only used for the consumer. Please do not use it in the processor.
func (s *SinkFactory) CreateTableSinkForConsumer(
	changefeedID model.ChangeFeedID,
	span tablepb.Span, startTs model.Ts,
	totalRowsCounter prometheus.Counter,
) tablesink.TableSink {
	if s.txnSink != nil {
		return tablesink.New(changefeedID, span, startTs, s.txnSink,
			// IgnoreStartTs is true because the consumer can
			// **not** get the start ts of the row changed event.
			&dmlsink.TxnEventAppender{TableSinkStartTs: startTs, IgnoreStartTs: true},
			totalRowsCounter)
	}

	return tablesink.New(changefeedID, span, startTs, s.rowSink,
		&dmlsink.RowChangeEventAppender{}, totalRowsCounter)
}

// Close closes the sink.
func (s *SinkFactory) Close() {
	if s.rowSink != nil && s.txnSink != nil {
		log.Panic("unreachable, rowSink and txnSink should not be both not nil")
	}
	if s.rowSink != nil {
		s.rowSink.Close()
	}
	if s.txnSink != nil {
		s.txnSink.Close()
	}
}

// Category returns category of s.
func (s *SinkFactory) Category() Category {
	if s.category == 0 {
		panic("should never happen")
	}
	return s.category
}

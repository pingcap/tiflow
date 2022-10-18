// Copyright 2020 PingCAP, Inc.
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

package sink

import (
	"context"
	"net/url"
	"strings"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/cdc/sink/mysql"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	// AddTable adds the table to MySQLSink or MQSink,
	// which is currently responsible for cleaning up
	// the residual values of the table in Sink.
	// See: https://github.com/pingcap/tiflow/issues/4464#issuecomment-1085385382.
	//
	// NOTICE: Only MySQLSink and MQSink implement it.
	// AddTable is thread-safe.
	AddTable(tableID model.TableID) error

	// EmitRowChangedEvents sends Row Changed Event to Sink
	// EmitRowChangedEvents may write rows to downstream directly;
	//
	// EmitRowChangedEvents is thread-safe.
	EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error

	// EmitDDLEvent sends DDL Event to Sink
	// EmitDDLEvent should execute DDL to downstream synchronously
	//
	// EmitDDLEvent is thread-safe.
	EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error

	// FlushRowChangedEvents flushes each row which of commitTs less than or
	// equal to `resolved.Ts` into downstream.
	// With `resolved.Mode == NormalResolvedMode`, TiCDC guarantees that all events whose commitTs
	// is less than or equal to `resolved.Ts` are sent to Sink.
	// With `resolved.Mode == BatchResolvedMode`, TiCDC guarantees that all events whose commitTs
	// is less than 'resolved.Ts' are sent to Sink.
	//
	// FlushRowChangedEvents is thread-safe.
	FlushRowChangedEvents(
		ctx context.Context, tableID model.TableID, resolved model.ResolvedTs,
	) (model.ResolvedTs, error)

	// EmitCheckpointTs sends CheckpointTs to Sink.
	// TiCDC guarantees that all Events **in the cluster** which of commitTs
	// less than or equal `checkpointTs` are sent to downstream successfully.
	// We use TableName instead of TableID here because
	// we need to consider the case of Table renaming.
	// Passing the TableName directly in the DDL Sink is a more straightforward solution.
	//
	// EmitCheckpointTs is thread-safe.
	EmitCheckpointTs(ctx context.Context, ts uint64, tables []*model.TableInfo) error

	// Close closes the Sink.
	//
	// Close is thread-safe and idempotent.
	Close(ctx context.Context) error

	// RemoveTable is a synchronous function to wait
	// all events of the table to be flushed in
	// underlying sink before we remove the table from sink.
	// Note once Barrier is called, the resolved ts **won't** be pushed until
	// the Barrier call returns.
	//
	// NOTICE: Only MySQLSink and MQSink implement it.
	// RemoveTable is thread-safe.
	RemoveTable(ctx context.Context, tableID model.TableID) error
}

var sinkIniterMap = make(map[string]sinkInitFunc)

type sinkInitFunc func(
	context.Context,
	model.ChangeFeedID,
	*url.URL,
	*config.ReplicaConfig,
	chan error,
) (Sink, error)

func init() {
	// register blackhole sink
	sinkIniterMap["blackhole"] = func(
		ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		config *config.ReplicaConfig,
		errCh chan error,
	) (Sink, error) {
		return newBlackHoleSink(ctx), nil
	}

	// register mysql sink
	sinkIniterMap["mysql"] = func(
		ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL, config *config.ReplicaConfig,
		errCh chan error,
	) (Sink, error) {
		return mysql.NewMySQLSink(ctx, changefeedID, sinkURI, config)
	}
	sinkIniterMap["tidb"] = sinkIniterMap["mysql"]
	sinkIniterMap["mysql+ssl"] = sinkIniterMap["mysql"]
	sinkIniterMap["tidb+ssl"] = sinkIniterMap["mysql"]

	// register kafka sink
	sinkIniterMap["kafka"] = func(
		ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		config *config.ReplicaConfig,
		errCh chan error,
	) (Sink, error) {
		return mq.NewKafkaSaramaSink(ctx, sinkURI, config, errCh, changefeedID)
	}
	sinkIniterMap["kafka+ssl"] = sinkIniterMap["kafka"]
}

// New creates a new sink with the sink-uri
func New(
	ctx context.Context, changefeedID model.ChangeFeedID, sinkURIStr string,
	config *config.ReplicaConfig,
	errCh chan error,
) (Sink, error) {
	// parse sinkURI as a URI
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	if err := config.ValidateAndAdjust(sinkURI); err != nil {
		return nil, err
	}
	if newSink, ok := sinkIniterMap[strings.ToLower(sinkURI.Scheme)]; ok {
		return newSink(ctx, changefeedID, sinkURI, config, errCh)
	}
	return nil, cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", sinkURI.Scheme)
}

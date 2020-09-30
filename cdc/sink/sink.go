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

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/cdclog"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
)

// Sink options keys
const (
	OptChangefeedID = "_changefeed_id"
	OptCaptureAddr  = "_capture_addr"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error

	// EmitRowChangedEvents sends Row Changed Event to Sink
	// EmitRowChangedEvents may write rows to downstream directly;
	EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error

	// EmitDDLEvent sends DDL Event to Sink
	// EmitDDLEvent should execute DDL to downstream synchronously
	EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error

	// FlushRowChangedEvents flushes each row which of commitTs less than or equal to `resolvedTs` into downstream.
	// TiCDC guarantees that all of Event which of commitTs less than or equal to `resolvedTs` are sent to Sink through `EmitRowChangedEvents`
	FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error)

	// EmitCheckpointTs sends CheckpointTs to Sink
	// TiCDC guarantees that all Events **in the cluster** which of commitTs less than or equal `checkpointTs` are sent to downstream successfully.
	EmitCheckpointTs(ctx context.Context, ts uint64) error

	// Close closes the Sink
	Close() error
}

// NewSink creates a new sink with the sink-uri
func NewSink(ctx context.Context, changefeedID model.ChangeFeedID, sinkURIStr string, filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
	// parse sinkURI as a URI
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	switch strings.ToLower(sinkURI.Scheme) {
	case "blackhole":
		return newBlackHoleSink(ctx, opts), nil
	case "mysql", "tidb", "mysql+ssl", "tidb+ssl":
		return newMySQLSink(ctx, changefeedID, sinkURI, filter, config, opts)
	case "kafka", "kafka+ssl":
		return newKafkaSaramaSink(ctx, sinkURI, filter, config, opts, errCh)
	case "pulsar", "pulsar+ssl":
		return newPulsarSink(ctx, sinkURI, filter, config, opts, errCh)
	case "local":
		return cdclog.NewLocalFileSink(ctx, sinkURI, errCh)
	case "s3":
		return cdclog.NewS3Sink(ctx, sinkURI, errCh)
	default:
		return nil, cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", sinkURI.Scheme)
	}
}

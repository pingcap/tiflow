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
	Close(ctx context.Context) error

	// Barrier is a synchronous function to wait all events to be flushed in underlying sink
	// Note once Barrier is called, the resolved ts won't be pushed until the Barrier call returns.
	Barrier(ctx context.Context) error
}

var sinkIniterMap = make(map[string]sinkInitFunc)

type sinkInitFunc func(context.Context, model.ChangeFeedID, *url.URL, *filter.Filter, *config.ReplicaConfig, map[string]string, chan error) (Sink, error)

func init() {
	// register blackhole sink
	sinkIniterMap["blackhole"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
		return newBlackHoleSink(ctx, opts), nil
	}

	// register mysql sink
	sinkIniterMap["mysql"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
		return newMySQLSink(ctx, changefeedID, sinkURI, filter, config, opts)
	}
	sinkIniterMap["tidb"] = sinkIniterMap["mysql"]
	sinkIniterMap["mysql+ssl"] = sinkIniterMap["mysql"]
	sinkIniterMap["tidb+ssl"] = sinkIniterMap["mysql"]

	// register kafka sink
	sinkIniterMap["kafka"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
		return newKafkaSaramaSink(ctx, sinkURI, filter, config, opts, errCh)
	}
	sinkIniterMap["kafka+ssl"] = sinkIniterMap["kafka"]

	// register pulsar sink
	sinkIniterMap["pulsar"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
		return newPulsarSink(ctx, sinkURI, filter, config, opts, errCh)
	}
	sinkIniterMap["pulsar+ssl"] = sinkIniterMap["pulsar"]

	// register local sink
	sinkIniterMap["local"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
		return cdclog.NewLocalFileSink(ctx, sinkURI, errCh)
	}

	// register s3 sink
	sinkIniterMap["s3"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
		filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
		return cdclog.NewS3Sink(ctx, sinkURI, errCh)
	}
}

// New creates a new sink with the sink-uri
func New(ctx context.Context, changefeedID model.ChangeFeedID, sinkURIStr string, filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
	// parse sinkURI as a URI
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	if newSink, ok := sinkIniterMap[strings.ToLower(sinkURI.Scheme)]; ok {
		return newSink(ctx, changefeedID, sinkURI, filter, config, opts, errCh)
	}
	return nil, cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", sinkURI.Scheme)
}

// Validate the health of the sink
func Validate(ctx context.Context, sinkURI string, cfg *config.ReplicaConfig, opts map[string]string) error {
	sinkFilter, err := filter.NewFilter(cfg)
	if err != nil {
		return err
	}
	errCh := make(chan error)
	// TODO: find a better way to verify a sinkURI is valid
	s, err := New(ctx, "sink-verify", sinkURI, sinkFilter, cfg, opts, errCh)
	if err != nil {
		return err
	}
	err = s.Close(ctx)
	if err != nil {
		return err
	}
	select {
	case err = <-errCh:
		if err != nil {
			return err
		}
	default:
	}
	return nil
}

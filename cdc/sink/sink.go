// Copyright 2019 PingCAP, Inc.
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

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
)

// Sink options keys
const (
	OptChangefeedID = "_changefeed_id"
	OptCaptureID    = "_capture_id"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	// EmitResolvedEvent saves the global resolved to the sink backend
	EmitResolvedEvent(ctx context.Context, ts uint64) error
	// EmitCheckpointEvent saves the global checkpoint to the sink backend
	EmitCheckpointEvent(ctx context.Context, ts uint64) error
	// EmitDMLs saves the specified DMLs to the sink backend
	EmitRowChangedEvent(ctx context.Context, rows ...*model.RowChangedEvent) error
	// EmitDDL saves the specified DDL to the sink backend
	EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error
	// CheckpointTs returns the sink checkpoint
	CheckpointTs() uint64
	// Count returns the count of sunk events
	Count() uint64
	// Run runs the sink
	Run(ctx context.Context) error
	// PrintStatus prints necessary status periodically
	PrintStatus(ctx context.Context) error
	Close() error
}

// DSNScheme is the scheme name of DSN
const DSNScheme = "dsn://"

// NewSink creates a new sink with the sink-uri
func NewSink(ctx context.Context, sinkURIStr string, filter *util.Filter, opts map[string]string) (Sink, error) {
	// check if sinkURI is a DSN
	if strings.HasPrefix(strings.ToLower(sinkURIStr), DSNScheme) {
		dsnStr := sinkURIStr[len(DSNScheme):]
		dsnCfg, err := dmysql.ParseDSN(dsnStr)
		if err != nil {
			return nil, errors.Annotatef(err, "parse sinkURI failed")
		}
		return newMySQLSink(nil, dsnCfg, filter, opts)
	}

	// parse sinkURI as a URI
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, errors.Annotatef(err, "parse sinkURI failed")
	}
	switch strings.ToLower(sinkURI.Scheme) {
	case "blackhole":
		return newBlackHoleSink(opts), nil
	case "mysql", "tidb":
		return newMySQLSink(sinkURI, nil, filter, opts)
	case "kafka":
		return newKafkaSaramaSink(ctx, sinkURI, filter, opts)
	default:
		return nil, errors.Errorf("the sink scheme (%s) is not supported", sinkURI.Scheme)
	}
}

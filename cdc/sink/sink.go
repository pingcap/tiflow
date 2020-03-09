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
)

// Sink options keys
const (
	OptDryRun       = "_dry_run"
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
	// Run runs the sink
	Run(ctx context.Context) error
	// Close does not guarantee delivery of outstanding messages.
	Close() error
	// PrintStatus prints necessary status periodically
	PrintStatus(ctx context.Context) error
}

// NewSink creates a new sink with the sink-uri
func NewSink(sinkURIStr string, opts map[string]string) (Sink, error) {
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		// try to parse the sinkURI as DSN
		dsnCfg, err := dmysql.ParseDSN(sinkURIStr)
		if err != nil {
			return nil, errors.Annotatef(err, "parse sinkURI failed")
		}
		return newMySQLSink(nil, dsnCfg, opts)
	}
	switch strings.ToLower(sinkURI.Scheme) {
	case "blackhole":
		return newBlackHoleSink(), nil
	case "mysql", "tidb":
		return newMySQLSink(sinkURI, nil, opts)
	case "kafka":
		return newKafkaSaramaSink(sinkURI)
	default:
		return nil, errors.Errorf("the sink scheme (%s) is not supported", sinkURI.Scheme)
	}
}

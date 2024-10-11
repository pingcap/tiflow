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

package syncpointstore

import (
	"context"
	"net/url"
	"strings"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// SyncPointStore is an abstraction for anything that a changefeed may emit into.
type SyncPointStore interface {
	// CreateSyncTable create a table to record the syncpoints
	CreateSyncTable(ctx context.Context) error

	// SinkSyncPoint record the syncpoint(a map with ts) in downstream db
	SinkSyncPoint(ctx context.Context, id model.ChangeFeedID, checkpointTs uint64) error

	// Close closes the SyncPointSink
	Close() error
}

// NewSyncPointStore creates a new SyncPoint sink with the sink-uri
func NewSyncPointStore(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURIStr string,
	replicaConfig *config.ReplicaConfig,
) (SyncPointStore, error) {
	// parse sinkURI as a URI
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	switch strings.ToLower(sinkURI.Scheme) {
	case "mysql", "tidb", "mysql+ssl", "tidb+ssl":
		return newMySQLSyncPointStore(ctx, changefeedID, sinkURI, replicaConfig)
	default:
		return nil, cerror.ErrSinkURIInvalid.
			GenWithStack("the sink scheme (%s) is not supported", sinkURI.Scheme)
	}
}

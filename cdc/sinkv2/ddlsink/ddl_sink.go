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
// limitations under the License.

package ddlsink

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
)

// DDLEventSink is the interface for sink of DDL events.
type DDLEventSink interface {
	// WriteDDLEvent writes a DDL event to the sink.
	// Note: This is a synchronous and thread-safe method.
	WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error
	// WriteCheckpointTs writes a checkpoint timestamp to the sink.
	// Note: This is a synchronous and thread-safe method.
	// This only for MQSink for now.
	WriteCheckpointTs(ctx context.Context, ts uint64, tables []*model.TableInfo) error
	// Close closes the sink.
	Close() error
}

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

package cloudstorage

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Assert Sink implementation
var _ ddlsink.Sink = (*DDLSink)(nil)

// DDLSink is a sink that sends DDL events to the cloud storage system.
type DDLSink struct {
	// id indicates which changefeed this sink belongs to.
	id model.ChangeFeedID
	// statistic is used to record the DDL metrics
	statistics *metrics.Statistics
	storage    storage.ExternalStorage
}

// NewDDLSink creates a ddl sink for cloud storage.
func NewDDLSink(ctx context.Context, sinkURI *url.URL) (*DDLSink, error) {
	storage, err := util.GetExternalStorageFromURI(ctx, sinkURI.String())
	if err != nil {
		return nil, err
	}

	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	d := &DDLSink{
		id:         changefeedID,
		storage:    storage,
		statistics: metrics.NewStatistics(ctx, sink.TxnSink),
	}

	return d, nil
}

// WriteDDLEvent writes the ddl event to the cloud storage.
func (d *DDLSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	writeFile := func(def cloudstorage.TableDefinition) error {
		encodedDef, err := def.MarshalWithQuery()
		if err != nil {
			return errors.Trace(err)
		}

		path, err := def.GenerateSchemaFilePath()
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("write ddl event to external storage",
			zap.String("path", path), zap.Any("ddl", ddl))
		return d.statistics.RecordDDLExecution(func() error {
			err1 := d.storage.WriteFile(ctx, path, encodedDef)
			if err1 != nil {
				return err1
			}

			return nil
		})
	}

	var def cloudstorage.TableDefinition
	def.FromDDLEvent(ddl)
	if err := writeFile(def); err != nil {
		return errors.Trace(err)
	}

	if ddl.Type == timodel.ActionExchangeTablePartition {
		// For exchange partition, we need to write the schema of the source table.
		var sourceTableDef cloudstorage.TableDefinition
		sourceTableDef.FromTableInfo(ddl.PreTableInfo, ddl.TableInfo.Version)
		return writeFile(sourceTableDef)
	}
	return nil
}

// WriteCheckpointTs writes the checkpoint ts to the cloud storage.
func (d *DDLSink) WriteCheckpointTs(ctx context.Context,
	ts uint64, tables []*model.TableInfo,
) error {
	ckpt, err := json.Marshal(map[string]uint64{"checkpoint-ts": ts})
	if err != nil {
		return errors.Trace(err)
	}
	err = d.storage.WriteFile(ctx, "metadata", ckpt)
	return errors.Trace(err)
}

// Close closes the sink.
func (d *DDLSink) Close() {
	if d.statistics != nil {
		d.statistics.Close()
	}
}

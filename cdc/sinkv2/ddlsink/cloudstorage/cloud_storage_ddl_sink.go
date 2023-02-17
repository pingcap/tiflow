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
	"fmt"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/util"
)

// Assert DDLEventSink implementation
var _ ddlsink.DDLEventSink = (*ddlSink)(nil)

type ddlSink struct {
	// id indicates which changefeed this sink belongs to.
	id model.ChangeFeedID
	// statistic is used to record the DDL metrics
	statistics *metrics.Statistics
	storage    storage.ExternalStorage
}

// NewCloudStorageDDLSink creates a ddl sink for cloud storage.
func NewCloudStorageDDLSink(ctx context.Context, sinkURI *url.URL) (*ddlSink, error) {
	storage, err := util.GetExternalStorageFromURI(ctx, sinkURI.String())
	if err != nil {
		return nil, err
	}

	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	d := &ddlSink{
		id:         changefeedID,
		storage:    storage,
		statistics: metrics.NewStatistics(ctx, sink.TxnSink),
	}

	return d, nil
}

func generateSchemaPath(def cloudstorage.TableDefinition) string {
	return fmt.Sprintf("%s/%s/%d/schema.json", def.Schema, def.Table, def.TableVersion)
}

func (d *ddlSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	var def cloudstorage.TableDefinition

	if ddl.TableInfo.TableInfo == nil {
		return nil
	}

	def.FromDDLEvent(ddl)
	encodedDef, err := json.MarshalIndent(def, "", "    ")
	if err != nil {
		return errors.Trace(err)
	}

	path := generateSchemaPath(def)
	err = d.statistics.RecordDDLExecution(func() error {
		err1 := d.storage.WriteFile(ctx, path, encodedDef)
		if err1 != nil {
			return err1
		}

		return nil
	})

	return errors.Trace(err)
}

func (d *ddlSink) WriteCheckpointTs(ctx context.Context,
	ts uint64, tables []*model.TableInfo,
) error {
	ckpt, err := json.Marshal(map[string]uint64{"checkpoint-ts": ts})
	if err != nil {
		return errors.Trace(err)
	}
	err = d.storage.WriteFile(ctx, "metadata", ckpt)
	return errors.Trace(err)
}

func (d *ddlSink) Close() {
	if d.statistics != nil {
		d.statistics.Close()
	}
}

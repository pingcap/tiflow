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
)

// Assert DDLEventSink implementation
var _ ddlsink.DDLEventSink = (*ddlSink)(nil)

type ddlSink struct {
	// id indicates which changefeed this sink belongs to.
	id model.ChangeFeedID
	// statistic is used to record the DDL metrics
	statistics *metrics.Statistics
	storage    storage.ExternalStorage
	tableCache map[int64]uint64
}

func NewCloudStorageDDLSink(ctx context.Context, sinkURI *url.URL) (*ddlSink, error) {
	// parse backend storage from sinkURI
	bs, err := storage.ParseBackend(sinkURI.String(), &storage.BackendOptions{})
	if err != nil {
		return nil, err
	}

	// create an external storage.
	storage, err := storage.New(ctx, bs, nil)
	if err != nil {
		return nil, err
	}
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)

	d := &ddlSink{
		id:         changefeedID,
		storage:    storage,
		tableCache: map[int64]uint64{},
		statistics: metrics.NewStatistics(ctx, sink.TxnSink),
	}

	return d, nil
}

func (d *ddlSink) generateSchemaPath(def tableDef) string {
	return fmt.Sprintf("%s/%s/%d/schema.json", def.Schema, def.Table, def.Version)
}

func (d *ddlSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	var def tableDef

	def.fromTableInfo(ddl.TableInfo)
	encodedDef, err := json.MarshalIndent(def, "", "    ")
	if err != nil {
		return errors.Trace(err)
	}

	path := d.generateSchemaPath(def)
	err = d.statistics.RecordDDLExecution(func() error {
		err1 := d.storage.WriteFile(ctx, path, encodedDef)
		if err != nil {
			return err1
		}

		d.tableCache[ddl.TableInfo.ID] = ddl.TableInfo.TableInfoVersion
		return nil
	})

	return errors.Trace(err)
}

func (d *ddlSink) WriteCheckpointTs(ctx context.Context,
	ts uint64, tables []*model.TableInfo,
) error {
	for _, table := range tables {
		ver, ok := d.tableCache[table.ID]
		if !ok || table.TableInfoVersion != ver {
			var def tableDef
			def.fromTableInfo(table)

			encodedDef, err := json.MarshalIndent(def, "", "    ")
			if err != nil {
				return errors.Trace(err)
			}

			path := d.generateSchemaPath(def)
			err = d.storage.WriteFile(ctx, path, encodedDef)
			if err != nil {
				return errors.Trace(err)
			}
			d.tableCache[table.ID] = table.TableInfoVersion
		}
	}

	ckpt, err := json.Marshal(map[string]uint64{"checkpoint-ts": ts})
	if err != nil {
		return errors.Trace(err)
	}
	err = d.storage.WriteFile(ctx, "metadata", ckpt)
	return errors.Trace(err)
}

func (d *ddlSink) Close() error {
	if d.statistics != nil {
		d.statistics.Close()
	}

	return nil
}

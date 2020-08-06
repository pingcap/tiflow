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

package cdclog

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/cdc/model"
)

const (
	tablePrefix = "t_"
	logMetaFile = "log.meta"

	ddlEventsDir    = "ddls"
	ddlEventsPrefix = "ddl"

	maxUint64 = ^uint64(0)
)

type logMeta struct {
	Names            map[int64]string `json:"names"`
	GlobalResolvedTS uint64           `json:"global_resolved_ts"`
}

// Marshal saves logMeta
func (l *logMeta) Marshal() ([]byte, error) {
	return json.Marshal(l)
}

func makeTableDirectoryName(tableID int64) string {
	return fmt.Sprintf("%s%d", tablePrefix, tableID)
}

func makeTableFileObject(tableID int64, commitTS uint64) string {
	return fmt.Sprintf("%s%d/%s", tablePrefix, tableID, makeTableFileName(commitTS))
}

func makeTableFileName(commitTS uint64) string {
	return fmt.Sprintf("cdclog.%d", commitTS)
}

func makeLogMetaContent(tableInfos []*model.SimpleTableInfo) *logMeta {
	meta := new(logMeta)
	names := make(map[int64]string)
	for _, table := range tableInfos {
		if table != nil {
			log.Info("[makeLogMetaContent]", zap.Reflect("table", table))
			names[table.TableID] = fmt.Sprintf("%s.%s", table.Schema, table.Table)
		}
	}
	meta.Names = names
	return meta
}

func makeDDLFileObject(commitTS uint64) string {
	return fmt.Sprintf("%s/%s", ddlEventsDir, makeDDLFileName(commitTS))
}

func makeDDLFileName(commitTS uint64) string {
	return fmt.Sprintf("%s.%d", ddlEventsPrefix, maxUint64-commitTS)
}

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

package blob

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
)

const (
	tablePrefix     = "t_"
	rowEventsPrefix = "row"
	logMetaFile     = "log.meta"

	ddlEventsDir    = "ddls"
	ddlEventsPrefix = "ddl"
)

type logMeta struct {
	Names            map[int64]string `json:"names"`
	GlobalResolvedTS uint64           `json:"global_resolved_ts"`
}

func newLogMeta() *logMeta {
	return &logMeta{
		Names: make(map[int64]string),
	}
}

// Marshal saves logMeta
func (l *logMeta) Marshal() ([]byte, error) {
	return json.Marshal(l)
}

func makeLogMetaContent(tables []model.TableName) *logMeta {
	meta := new(logMeta)
	names := make(map[int64]string)
	for _, table := range tables {
		names[table.TableID] = table.QuoteString()
	}
	meta.Names = names
	return meta
}

func makeBlobStorageContent(mqMessages []*codec.MQMessage) []byte {
	fileBuf := new(bytes.Buffer)
	for _, msg := range mqMessages {
		keyIndex := uint64(8)
		valueIndex := uint64(0)
		for keyIndex < uint64(len(msg.Key)) {
			keyLen := binary.BigEndian.Uint64(msg.Key[keyIndex : keyIndex+8])
			fileBuf.Write(msg.Key[keyIndex : keyIndex+keyLen+8])
			keyIndex += keyLen + 8

			valueLen := binary.BigEndian.Uint64(msg.Value[valueIndex : valueIndex+8])
			fileBuf.Write(msg.Value[valueIndex : valueIndex+valueLen+8])
			valueIndex += valueLen + 8
		}
	}
	return fileBuf.Bytes()
}

func makeTableFileObject(tableID int64, commitTS uint64) string {
	return fmt.Sprintf("%s%d/%s", tablePrefix, tableID, makeTableFileName(commitTS))
}

func makeTableFileName(commitTS uint64) string {
	return fmt.Sprintf("%s.%d", rowEventsPrefix, commitTS)
}

func makeDDLFileObject(commitTS uint64) string {
	return fmt.Sprintf("%s/%s", ddlEventsDir, makeDDLFileName(commitTS))
}

func makeDDLFileName(commitTS uint64) string {
	return fmt.Sprintf("%s.%d", ddlEventsPrefix, commitTS)
}

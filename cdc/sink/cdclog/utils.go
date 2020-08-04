package cdclog

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/ticdc/cdc/model"
)

const (
	tablePrefix   = "t_"
	logMetaFile   = "log.meta"
	DDLEventsDir  = "ddls"
	DDLEventsFile = "ddl"

	maxUint64 = ^uint64(0)
)

type LogMeta struct {
	names            map[int64]string `json:"names"`
	globalResolvedTS uint64           `json:"global_resolved_ts"`
}

func (l *LogMeta) Marshal() ([]byte, error) {
	return json.Marshal(l)
}

func makeTableDirectoryName(tableID int64) string {
	return fmt.Sprintf("%s%d", tablePrefix, tableID)
}

func makeTableFileName(tableID int64, commitTS uint64) string {
	return fmt.Sprintf("%s%d/cdclog.%d", tablePrefix, tableID, commitTS)
}

func makeLogMetaContent(tableInfos []*model.TableInfo) *LogMeta {
	meta := new(LogMeta)
	names := make(map[int64]string)
	for _, table := range tableInfos {
		if table != nil {
			names[table.TableID] = fmt.Sprintf("%s.%s", table.Schema, table.Table)
		}
	}
	meta.names = names
	return meta
}

func makeDDLFileName(commitTS uint64) string {
	return fmt.Sprintf("%s/%s.%d", DDLEventsDir, DDLEventsFile, maxUint64-commitTS)
}

package logBackup

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/ticdc/cdc/model"
)

const (
	tablePrefix = "t_"
	logMetaFile = "log.meta"
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

func makeTableFileName(tableID int64, commmitTS uint64) string {
	return fmt.Sprintf("%s%d.%d", tablePrefix, tableID, commmitTS)
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

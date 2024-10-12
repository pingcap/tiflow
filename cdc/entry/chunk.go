package entry

import (
	"bytes"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const (
	RowTypeDelete = iota
	RowTypeInsert
	RowTypeUpdate
)

type TxnEvent struct {
	TableInfo       *model.TableInfo `json:"table_info"`
	PhysicalTableID int64            `json:"physical_table_id"`
	StartTs         uint64           `json:"start_ts"`
	CommitTs        uint64           `json:"commit_ts"`
	Rows            *chunk.Chunk     `json:"rows"`
	RowType         []int            `json:"row_type"`
}

func rawKVToChunk(raw *model.RawKVEntry, tableInfo *model.TableInfo, tz *time.Location, rowCount int) *chunk.Chunk {
	recordID, err := tablecodec.DecodeRowKey(raw.Key)
	if err != nil {
		log.Panic("decode row key failed", zap.Error(err))
	}
	if !bytes.HasPrefix(raw.Key, tablePrefix) {
		return nil
	}
	// key, physicalTableID, err := decodeTableID(raw.Key)
	// if err != nil {
	// 	return nil
	// }
	if len(raw.OldValue) == 0 && len(raw.Value) == 0 {
		return nil
	}
	handleColIDs, _, reqCols := tableInfo.GetRowColInfos()
	// This function is used to set the default value for the column that
	// is not in the raw data.
	defVal := func(i int, chk *chunk.Chunk) error {
		if reqCols[i].ID < 0 {
			// model.ExtraHandleID, ExtraPidColID, ExtraPhysTblID... etc
			// Don't set the default value for that column.
			chk.AppendNull(i)
			return nil
		}
		ci, ok := tableInfo.GetColumnInfo(reqCols[i].ID)
		if !ok {
			log.Panic("column not found", zap.Int64("columnID", reqCols[i].ID))
		}

		colDatum, _, _, warn, err := getDefaultOrZeroValue(ci, tz)
		if err != nil {
			return err
		}
		if warn != "" {
			log.Warn(warn, zap.String("table", tableInfo.TableName.String()),
				zap.String("column", ci.Name.String()))
		}
		chk.AppendDatum(i, &colDatum)
		return nil
	}
	chunkDecoder := rowcodec.NewChunkDecoder(reqCols, handleColIDs, defVal, tz)
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFileSlice(), rowCount)
	for i := 0; i < rowCount; i++ {
		err = chunkDecoder.DecodeToChunk(raw.Value, recordID, chk)
	}
	if err != nil {
		log.Panic("decode row failed", zap.Error(err))
	}
	return chk
}

type row struct {
	Columns    []*model.ColumnData `json:"columns"`
	PreColumns []*model.ColumnData `json:"pre_columns"`
}

func (r row) String() string {
	sb := strings.Builder{}
	sb.WriteString("row: ")
	for _, col := range r.Columns {
		sb.WriteString(col.String())
	}
	return sb.String()
}

func chunkToRows(chk *chunk.Chunk, tableInfo *model.TableInfo) []row {
	rows := make([]row, 0, chk.NumRows())
	fieldSlice := tableInfo.GetFileSlice()
	for i := 0; i < chk.NumRows(); i++ {
		row := row{Columns: make([]*model.ColumnData, 0, chk.NumCols())}
		for j := 0; j < chk.NumCols(); j++ {
			col := chk.GetRow(i).GetDatum(j, fieldSlice[j])
			rv := col.GetValue()
			//v, _, _, _ := formatColVal(col, tableInfo.Columns[j])
			row.Columns = append(row.Columns, &model.ColumnData{
				ColumnID: tableInfo.Columns[j].ID,
				Value:    rv,
			})
		}
		rows = append(rows, row)
	}
	return rows
}

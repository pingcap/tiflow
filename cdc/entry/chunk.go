package entry

import (
	"bytes"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

func rawKVToChunk(raw *model.RawKVEntry, tableInfo *model.TableInfo, tz *time.Location) *chunk.Chunk {

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
		zap.Any("row", raw)
	}

	handleColIDs, rowColFieldTps, reqCols := tableInfo.GetRowColInfos()

	rowColFieldTpsSlice := make([]*types.FieldType, 0, len(rowColFieldTps))
	for _, ft := range rowColFieldTps {
		rowColFieldTpsSlice = append(rowColFieldTpsSlice, ft)
	}
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
	chk := chunk.NewChunkWithCapacity(rowColFieldTpsSlice, 1)
	err = chunkDecoder.DecodeToChunk(raw.Value, recordID, chk)

	if err != nil {
		log.Panic("decode row failed", zap.Error(err))
	}

	fmt.Println("fizz chunk: ", chk.ToString(rowColFieldTpsSlice), "chunk size: ", chk.MemoryUsage())

	return chk
}

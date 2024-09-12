package txn

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// BenchmarkGenRowKeysNew-10: 3397686	       335.3 ns/op	     266 B/op	       9 allocs/op
func BenchmarkGenRowKeysNew(b *testing.B) {
	helper := entry.NewSchemaTestHelper(b)
	defer helper.Close()

	helper.DDL2Event(`
		CREATE TABLE test.t1 (
			id INT PRIMARY KEY,
			col1 VARCHAR(255) NOT NULL,
			col2 VARCHAR(255) NOT NULL,
			col3 VARCHAR(255) NOT NULL,
			col4 VARCHAR(255) NOT NULL,
			col5 VARCHAR(255) NOT NULL,
			col6 VARCHAR(255) NOT NULL,
			col7 VARCHAR(255),
			col8 VARCHAR(255),
			col9 VARCHAR(255),
			col10 VARCHAR(255),
			col11 VARCHAR(255),
			col12 VARCHAR(255),
			col13 VARCHAR(255),
			col14 VARCHAR(255),
			col15 VARCHAR(255),
			col16 VARCHAR(255),
			col17 VARCHAR(255),
			col18 VARCHAR(255),
			col19 VARCHAR(255),
			UNIQUE KEY uk1 (col1, col2, col3),
			UNIQUE KEY uk2 (col4, col5, col6)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
	`)

	row := helper.DML2Event("insert into test.t1 values (1, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's')", "test", "t1")

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		genRowKeysNew(row)
	}
}

// BenchmarkGenRowKeys-10:   317608	      3835 ns/op	    7492 B/op	      72 allocs/op
func BenchmarkGenRowKeys(b *testing.B) {
	helper := entry.NewSchemaTestHelper(b)
	defer helper.Close()

	helper.DDL2Event(`
		CREATE TABLE test.t1 (
			id INT PRIMARY KEY,
			col1 VARCHAR(255) NOT NULL,
			col2 VARCHAR(255) NOT NULL,
			col3 VARCHAR(255) NOT NULL,
			col4 VARCHAR(255) NOT NULL,
			col5 VARCHAR(255) NOT NULL,
			col6 VARCHAR(255) NOT NULL,
			col7 VARCHAR(255),
			col8 VARCHAR(255),
			col9 VARCHAR(255),
			col10 VARCHAR(255),
			col11 VARCHAR(255),
			col12 VARCHAR(255),
			col13 VARCHAR(255),
			col14 VARCHAR(255),
			col15 VARCHAR(255),
			col16 VARCHAR(255),
			col17 VARCHAR(255),
			col18 VARCHAR(255),
			col19 VARCHAR(255),
			UNIQUE KEY uk1 (col1, col2, col3),
			UNIQUE KEY uk2 (col4, col5, col6)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
	`)

	row := helper.DML2Event("insert into test.t1 values (1, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's')", "test", "t1")

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		genRowKeys(row)
	}
}

func genRowKeysNew(row *model.RowChangedEvent) [][]byte {
	var keys [][]byte
	if len(row.Columns) != 0 {
		for iIdx, idxCol := range row.TableInfo.IndexColumnsOffset {
			key := genKeyListNew(row, iIdx, idxCol, row.GetTableID())
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if len(row.PreColumns) != 0 {
		for iIdx, idxCol := range row.TableInfo.IndexColumnsOffset {
			key := genKeyListNew(row, iIdx, idxCol, row.GetTableID())
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		// use table ID as key if no key generated (no PK/UK),
		// no concurrence for rows in the same table.
		log.Debug("Use table id as the key", zap.Int64("tableID", row.GetTableID()))
		tableKey := make([]byte, 8)
		binary.BigEndian.PutUint64(tableKey, uint64(row.GetTableID()))
		keys = [][]byte{tableKey}
	}
	return keys
}

func genKeyListNew(
	row *model.RowChangedEvent, iIdx int, colIdx []int, tableID int64,
) []byte {
	var key []byte
	for _, i := range colIdx {
		colData := row.Columns[i]
		colInfo := row.TableInfo.Columns[i]
		// if a column value is null, we can ignore this index
		// If the index contain generated column, we can't use this key to detect conflict with other DML,
		// Because such as insert can't specify the generated value.
		if colData == nil ||
			colData.Value == nil ||
			row.TableInfo.ColumnsFlag[colData.ColumnID].IsGeneratedColumn() {
			return nil
		}

		if colInfo.ID != colData.ColumnID {
			log.Panic("column id not match", zap.Int64("id", colInfo.ID), zap.Int64("columnID", colData.ColumnID))
		}

		val := model.ColumnValueString(colData.Value)
		if columnNeeds2LowerCase(
			colInfo.GetType(),
			colInfo.GetCollate()) {
			val = strings.ToLower(val)
		}

		key = append(key, []byte(val)...)
		key = append(key, 0)
	}
	if len(key) == 0 {
		return nil
	}
	tableKey := make([]byte, 16)
	binary.BigEndian.PutUint64(tableKey[:8], uint64(iIdx))
	binary.BigEndian.PutUint64(tableKey[8:], uint64(tableID))
	key = append(key, tableKey...)
	return key
}

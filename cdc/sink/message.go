package sink

import (
	"bufio"
	"encoding/json"
	"errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/encoding"
	"hash"
)

const (
	// MagicIndex 4 bytes at the head of message.
	MagicIndex = 0xBAAAD700

	// Version represents version of message.
	Version = 1

	// DmlType represents of message.
	DmlType = byte(0)

	// DdlType represents of message.
	DdlType = byte(1)
)


type Writer struct {
	fbuf *bufio.Writer

	// Reusable memory.
	buf1    encoding.Encbuf
	buf2    encoding.Encbuf
	uint32s []uint32
	crc32 hash.Hash
}

type Datum struct {
	Value interface{} `json:"value"`
}


func (w *Writer) writeMeta() error{
	w.buf1.Reset()
	w.buf1.PutBE32(MagicIndex)
	w.buf1.PutByte(Version)

	return w.write(w.buf1.Get())
}

func (w *Writer) write(bufs ...[]byte) error {
	for _, b := range bufs {
		if _, err := w.fbuf.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) WriteTxn(txn model.Txn, infoGetter TableInfoGetter) error{
	w.buf1.Reset()
	w.buf1.PutBE64(txn.Ts)

	if txn.IsDDL() {
		w.buf1.PutByte(DdlType)

		w.buf2.Reset()
		w.buf2.PutUvarintStr(txn.DDL.Database)
		w.buf2.PutUvarintStr(txn.DDL.Table)

		job, err := json.Marshal(txn.DDL.Job)
		if err != nil {
			return err
		}

		w.buf2.PutUvarintStr(string(job))

		w.buf1.PutBE32int(w.buf2.Len())
		w.buf2.PutHash(w.crc32)

		return w.write(w.buf1.Get(), w.buf2.Get())
	}

	return w.writeDML(txn.DMLs, infoGetter)
}

func (w *Writer) writeDML(dmls []*model.DML, infoGetter TableInfoGetter) error{
	w.buf1.PutByte(DmlType)
	w.buf1.PutBE32int(len(dmls))

	w.buf2.Reset()
	for _, dml := range dmls {
		// write dml meta
		w.buf2.PutUvarintStr(dml.Database)
		w.buf2.PutUvarintStr(dml.Table)
		w.buf2.PutBE32int(int(dml.Tp))

		//write values
		w.buf2.PutBE32int(len(dml.Values))
		for key, value := range dml.Values {
			w.buf2.PutUvarintStr(key)

			v, err := json.Marshal(Datum{
				Value: value.GetValue(),
			})
			if err != nil {
				return err
			}

			w.buf2.PutUvarintStr(string(v))
		}

		tblID, ok := infoGetter.GetTableIDByName(dml.Database, dml.Table)
		if !ok {
			return errors.New("get table id failed")
		}

		tableInfo, ok := infoGetter.TableByID(tblID)
		if !ok {
			return errors.New("get table by id failed")
		}

		//write columns
		columns := writableColumns(tableInfo)
		w.buf2.PutBE32int(len(columns))
		for _, c := range columns {
			cbytes, err := json.Marshal(c)
			if err != nil {
				return err
			}

			w.buf2.PutUvarintStr(string(cbytes))
		}
	}

	w.buf1.PutBE32int(w.buf2.Len())
	w.buf2.PutHash(w.crc32)

	return w.write(w.buf1.Get(), w.buf2.Get())
}


// writableColumns returns all columns which can be written. This excludes
// generated and non-public columns.
func writableColumns(table *timodel.TableInfo) []*timodel.ColumnInfo {
	cols := make([]*timodel.ColumnInfo, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.State == timodel.StatePublic && !col.IsGenerated() {
			cols = append(cols, col)
		}
	}
	return cols
}
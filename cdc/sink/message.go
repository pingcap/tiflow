package sink

import (
	"encoding/json"
	"errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/encoding"
	"hash"
	"hash/crc32"
)

// Message tyupe
const (
	// emit resolve type message.
	ResolveTsType = iota
	// txn message.
	TxnType
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

type writer struct {
	content []byte

	// Reusable memory.
	buf1    encoding.Encbuf
	buf2    encoding.Encbuf

	version byte
	msgType int
}

func (w *writer) write(bufs ...[]byte) error {
	for _, b := range bufs {
		w.content = append(w.content, b...)
	}
	return nil
}

func (w *writer) writeMeta() error{
	w.buf1.Reset()
	w.buf1.PutBE32(MagicIndex)
	w.buf1.PutByte(w.version)
	w.buf1.PutBE32int(w.msgType)

	return w.write(w.buf1.Get())
}

func (w *writer) flush() []byte {
	return w.content
}


type resolveTsWriter struct {
	*writer
	ts int64
}

func NewResloveTsWriter(ts int64)  *resolveTsWriter{
	return &resolveTsWriter{
		writer: &writer{
			version: Version,
			msgType: ResolveTsType,
			content: make([]byte, 0, 1 << 22),
			buf1:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
			buf2:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
		},
		ts: ts,
	}
}

func (w *resolveTsWriter) Write() ([]byte, error) {
	w.writeMeta()
	w.buf1.Reset()
	w.buf1.PutBE64int64(w.ts)
	err := w.write(w.buf1.Get())
	if err != nil {
		return nil, err
	}
	
	return w.flush(), nil
}

type txnWriter struct {
	*writer
	crc32 hash.Hash
	txn model.Txn
	infoGetter TableInfoGetter
}

func (w *txnWriter) NewTxnWriter(txn model.Txn, infoGetter TableInfoGetter) *txnWriter{
	return &txnWriter{
		writer: &writer{
			version: Version,
			msgType: TxnType,
			content: make([]byte, 0, 1 << 22),
			buf1:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
			buf2:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
		},
		crc32: crc32.New(castagnoliTable),
		txn: txn,
		infoGetter: infoGetter,
	}
}

var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

type Datum struct {
	Value interface{} `json:"value"`
}

func (w *txnWriter) WriteTxn() error{
	w.buf1.Reset()
	w.buf1.PutBE64(w.txn.Ts)

	if w.txn.IsDDL() {
		w.buf1.PutByte(DdlType)

		w.buf2.Reset()
		w.buf2.PutUvarintStr(w.txn.DDL.Database)
		w.buf2.PutUvarintStr(w.txn.DDL.Table)

		job, err := json.Marshal(w.txn.DDL.Job)
		if err != nil {
			return err
		}

		w.buf2.PutUvarintStr(string(job))

		w.buf1.PutBE32int(w.buf2.Len())
		w.buf2.PutHash(w.crc32)

		return w.write(w.buf1.Get(), w.buf2.Get())
	}

	return w.writeDML(w.txn.DMLs, w.infoGetter)
}

func (w *txnWriter) writeDML(dmls []*model.DML, infoGetter TableInfoGetter) error{
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

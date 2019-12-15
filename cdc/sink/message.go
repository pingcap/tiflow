package sink

import (
	"encoding/json"
	"errors"
	"fmt"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/encoding"
	"github.com/pingcap/tidb/types"
	"hash"
	"hash/crc32"
)

const (
	// MagicIndex 4 bytes at the head of message.
	MagicIndex = 0xBAAAD700

	// Version represents version of message.
	Version = 1
)

type TxnOp byte

const (
	_ = iota
	// DmlType represents of message.
	DmlOp TxnOp = 1 + iota

	// DdlType represents of message.
	DdlOp
)

type writer struct {
	content []byte

	// Reusable memory.
	buf1 encoding.Encbuf
	buf2 encoding.Encbuf

	version byte
	msgType MsgType
	cdcId   string
}

func (w *writer) write(bufs ...[]byte) error {
	for _, b := range bufs {
		w.content = append(w.content, b...)
	}
	return nil
}

func (w *writer) writeMeta() error {
	w.buf1.Reset()
	w.buf1.PutBE32(MagicIndex)
	w.buf1.PutByte(w.version)
	w.buf1.PutByte(byte(w.msgType))
	w.buf1.PutUvarintStr(w.cdcId)

	return w.write(w.buf1.Get())
}

func (w *writer) flush() []byte {
	return w.content
}

type metaWriter struct {
	*writer
	cdcIds         []string
	broadcastCount int
	crc32          hash.Hash
}

func NewMetaWriter(cdcIds []string, broadcastCount int) *metaWriter {
	return &metaWriter{
		writer: &writer{
			version: Version,
			msgType: MetaType,
			content: make([]byte, 0, 1<<22),
			buf1:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
			buf2:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
			cdcId:   "",
		},
		cdcIds:         cdcIds,
		broadcastCount: broadcastCount,
		crc32:          crc32.New(castagnoliTable),
	}
}

func (w *metaWriter) Write() ([]byte, error) {
	if err := w.writeMeta(); err != nil {
		return nil, err
	}

	w.buf2.Reset()
	w.buf2.PutBE32int(len(w.cdcIds))
	for _, cdc := range w.cdcIds {
		w.buf2.PutUvarintStr(cdc)
	}

	w.buf2.PutBE32int(w.broadcastCount)

	w.buf1.Reset()
	w.buf1.PutBE32int(w.buf2.Len())

	w.buf2.PutHash(w.crc32)

	err := w.write(w.buf1.Get(), w.buf2.Get())
	if err != nil {
		return nil, err
	}

	return w.flush(), nil
}

type resolveTsWriter struct {
	*writer
	ts uint64
}

func NewResloveTsWriter(cdcId string, ts uint64) *resolveTsWriter {
	return &resolveTsWriter{
		writer: &writer{
			version: Version,
			msgType: ResolveTsType,
			content: make([]byte, 0, 1<<22),
			buf1:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
			buf2:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
			cdcId:   cdcId,
		},
		ts: ts,
	}
}

func (w *resolveTsWriter) Write() ([]byte, error) {
	if err := w.writeMeta(); err != nil {
		return nil, err
	}

	w.buf1.Reset()
	w.buf1.PutBE64(w.ts)
	err := w.write(w.buf1.Get())
	if err != nil {
		return nil, err
	}

	return w.flush(), nil
}

type txnWriter struct {
	*writer
	crc32      hash.Hash
	txn        model.Txn
	infoGetter TableInfoGetter
}

func NewTxnWriter(cdcId string, txn model.Txn, infoGetter TableInfoGetter) *txnWriter {
	return &txnWriter{
		writer: &writer{
			version: Version,
			msgType: TxnType,
			content: make([]byte, 0, 1<<22),
			buf1:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
			buf2:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
			cdcId:   cdcId,
		},
		crc32:      crc32.New(castagnoliTable),
		txn:        txn,
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

func (w *txnWriter) Write() ([]byte, error) {
	w.writeMeta()
	w.buf1.Reset()
	w.buf1.PutBE64(w.txn.Ts)

	if w.txn.IsDDL() {
		w.buf1.PutByte(byte(DdlOp))

		w.buf2.Reset()
		w.buf2.PutUvarintStr(w.txn.DDL.Database)
		w.buf2.PutUvarintStr(w.txn.DDL.Table)

		job, err := json.Marshal(w.txn.DDL.Job)
		if err != nil {
			return nil, err
		}

		w.buf2.PutUvarintStr(string(job))

		w.buf1.PutBE32int(w.buf2.Len())
		w.buf2.PutHash(w.crc32)

		if err := w.write(w.buf1.Get(), w.buf2.Get()); err != nil {
			return nil, err
		}

		return w.flush(), nil
	}

	if err := w.writeDML(w.txn.DMLs, w.infoGetter); err != nil {
		return nil, err
	}

	return w.flush(), nil
}

func (w *txnWriter) writeDML(dmls []*model.DML, infoGetter TableInfoGetter) error {
	w.buf1.PutByte(byte(DmlOp))
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

		// write tableInfo
		cbytes, err := json.Marshal(tableInfo)
		if err != nil {
			return err
		}

		w.buf2.PutUvarintStr(string(cbytes))
	}

	w.buf1.PutBE32int(w.buf2.Len())
	w.buf2.PutHash(w.crc32)

	return w.write(w.buf1.Get(), w.buf2.Get())
}

type reader struct {
	data []byte
}

func NewReader(data []byte) *reader {
	return &reader{
		data: data,
	}
}

func (r *reader) Decode() (*Message, error) {
	d := &encoding.Decbuf{B: r.data}
	if d.Be32() != MagicIndex {
		return nil, errors.New("invalid message format")
	}

	// version
	d.Byte()

	//type
	t := d.Byte()
	switch MsgType(t) {
	case ResolveTsType:
		return r.decodeResloveTsMsg(d), nil
	case TxnType:
		return r.decodeTxnMsg(d)
	case MetaType:
		return r.decodeMeta(d), nil
	default:
		return nil, errors.New(fmt.Sprintf("unsupport type - %d", t))
	}
}

func (r *reader) decodeMeta(d *encoding.Decbuf) *Message {
	m := &Message{
		CdcID:   d.UvarintStr(),
		MsgType: MetaType,
	}

	d.Be32int()
	cdcLen := d.Be32int()
	cdcIds := make([]string, cdcLen)
	for i := 0; i < cdcLen; i++ {
		cdcIds[i] = d.UvarintStr()
	}
	m.CdcList = cdcIds
	m.MetaCount = d.Be32int()

	return m
}

func (r *reader) decodeResloveTsMsg(d *encoding.Decbuf) *Message {
	return &Message{
		CdcID:     d.UvarintStr(),
		MsgType:   ResolveTsType,
		ResloveTs: d.Be64(),
	}
}

func (r *reader) decodeTxnMsg(d *encoding.Decbuf) (*Message, error) {
	m := &Message{
		CdcID:   d.UvarintStr(),
		MsgType: TxnType,
	}

	ts := d.Be64()
	switch TxnOp(d.Byte()) {
	case DmlOp:
		txn, tinfos, err := r.decodeDML(d, ts)
		if err != nil {
			return nil, err
		}
		m.Txn = txn
		m.TableInfos = tinfos
		return m, nil
	case DdlOp:
		txn, err := r.decodeDDL(d, ts)
		if err != nil {
			return nil, err
		}

		m.Txn = txn
		return m, nil
	default:
		return nil, errors.New(fmt.Sprintf("unsupport txn operator"))
	}
}

func (r *reader) decodeDDL(d *encoding.Decbuf, ts uint64) (*model.Txn, error) {
	txn := &model.Txn{
		Ts: ts,
	}

	// TOOD check checksum
	d.Be32int()

	txn.DDL = &model.DDL{
		Database: d.UvarintStr(),
		Table:    d.UvarintStr(),
		Job:      &timodel.Job{},
	}

	job := d.UvarintStr()

	err := json.Unmarshal([]byte(job), txn.DDL.Job)
	if err != nil {
		return nil, err
	}

	return txn, nil
}

func (r *reader) decodeDML(d *encoding.Decbuf, ts uint64) (*model.Txn, map[string]*timodel.TableInfo, error) {
	txn := &model.Txn{
		Ts: ts,
	}

	dmlLen := d.Be32int()
	// TOOD check checksum
	d.Be32int()

	talbeInfoMap := make(map[string]*timodel.TableInfo)
	dmls := make([]*model.DML, dmlLen)
	for i := 0; i < dmlLen; i++ {
		database := d.UvarintStr()
		table := d.UvarintStr()
		dml := &model.DML{
			Database: database,
			Table:    table,
			Tp:       model.DMLType(d.Be32int()),
		}

		values, err := r.decodeDMLValues(d)
		if err != nil {
			return nil, nil, err
		}
		dml.Values = values
		dmls[i] = dml

		// table Info
		var tbl timodel.TableInfo
		err = json.Unmarshal([]byte(d.UvarintStr()), &tbl)
		if err != nil {
			return nil, nil, err
		}

		talbeInfoMap[FormMapKey(database, table)] = &tbl
	}

	txn.DMLs = dmls

	return txn, talbeInfoMap, nil
}

func FormMapKey(database, table string) string {
	return fmt.Sprintf("%s.%s", database, table)
}

func (r *reader) decodeDMLValues(d *encoding.Decbuf) (map[string]types.Datum, error) {
	valueLen := d.Be32int()
	m := make(map[string]types.Datum, valueLen)
	for i := 0; i < valueLen; i++ {
		key := d.UvarintStr()

		var dt Datum
		err := json.Unmarshal([]byte(d.UvarintStr()), &dt)
		if err != nil {
			return nil, err
		}

		td := &types.Datum{}
		td.SetInterface(dt.Value)
		m[key] = *td
	}

	return m, nil
}

// Copyright 2023 PingCAP, Inc.
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

package codec

import (
	"bytes"
	"encoding/binary"

	pmodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	codecv1 "github.com/pingcap/tiflow/cdc/model/codec/v1"
	"github.com/tinylib/msgp/msgp"
)

const (
	v1HeaderLength      int = 4
	versionPrefixLength int = 2
	versionFieldLength  int = 2

	latestVersion uint16 = 2
)

// NOTE: why we need this?
//
// Before this logic is introduced, redo log is encoded into byte slice without a version field.
// This makes it hard to extend in future.
// However in the old format (i.e. v1 format), the first 5 bytes are always same, which can be
// confirmed in v1/codec_gen.go. So we reuse those bytes, and add a version field in them.
var (
	v1Header      [v1HeaderLength]byte      = [...]byte{0x83, 0xa3, 0x72, 0x6f}
	versionPrefix [versionPrefixLength]byte = [...]byte{0xff, 0xff}
)

func postUnmarshal(r *model.RedoLog) {
	if r.RedoRow.Row != nil {
		row := r.RedoRow.Row
		for i, c := range row.Columns {
			c.Flag = model.ColumnFlagType(r.RedoRow.Columns[i].Flag)
			if r.RedoRow.Columns[i].ValueIsEmptyByteSlice {
				c.Value = []byte{}
			} else {
				c.Value = r.RedoRow.Columns[i].Value
			}
		}
		for i, c := range row.PreColumns {
			c.Flag = model.ColumnFlagType(r.RedoRow.PreColumns[i].Flag)
			if r.RedoRow.Columns[i].ValueIsEmptyByteSlice {
				c.Value = []byte{}
			} else {
				c.Value = r.RedoRow.Columns[i].Value
			}
		}
	}
	if r.RedoDDL.DDL != nil {
		r.RedoDDL.DDL.Type = pmodel.ActionType(r.RedoDDL.Type)
	}
}

func preMarshal(r *model.RedoLog) {
	if r.RedoRow.Row != nil {
		row := r.RedoRow.Row
		r.RedoRow.Columns = make([]model.RedoColumn, 0, len(row.Columns))
		for _, c := range row.Columns {
			redoC := model.RedoColumn{Value: c.Value, Flag: uint64(c.Flag)}
			// Workaround empty byte slice for msgp#247
			switch v := redoC.Value.(type) {
			case []byte:
				if bytes.Equal(v, []byte{}) {
					redoC.ValueIsEmptyByteSlice = true
				}
			}
			r.RedoRow.Columns = append(r.RedoRow.Columns, redoC)
		}
		r.RedoRow.PreColumns = make([]model.RedoColumn, 0, len(row.PreColumns))
		for _, c := range row.PreColumns {
			redoC := model.RedoColumn{Value: c.Value, Flag: uint64(c.Flag)}
			// Workaround empty byte slice for msgp#247
			switch v := redoC.Value.(type) {
			case []byte:
				if bytes.Equal(v, []byte{}) {
					redoC.ValueIsEmptyByteSlice = true
				}
			}
			r.RedoRow.PreColumns = append(r.RedoRow.PreColumns, redoC)
		}
	}
	if r.RedoDDL.DDL != nil {
		r.RedoDDL.Type = byte(r.RedoDDL.DDL.Type)
	}
}

func DecodeRedoLog(dc *msgp.Reader) (r *model.RedoLog, err error) {
	var p []byte
	p, err = dc.R.Peek(versionPrefixLength)
	if err != nil {
		return
	}

	shouldBeV1 := false
	for i := 0; i < versionPrefixLength; i++ {
		if p[i] != versionPrefix[i] {
			shouldBeV1 = true
			break
		}
	}

	if shouldBeV1 {
		var rv1 *codecv1.RedoLog = new(codecv1.RedoLog)
		if err = rv1.DecodeMsg(dc); err != nil {
			return
		}
		codecv1.PostUnmarshal(rv1)
		r = redoLogFromV1(rv1)
	} else {
		p = make([]byte, 4)
		if _, err = dc.R.Read(p); err != nil {
			return
		}
		version, _ := decodeVersion(p[versionPrefixLength:])
		if version == latestVersion {
			r = new(model.RedoLog)
			if err = r.DecodeMsg(dc); err != nil {
				return
			}
			postUnmarshal(r)
		} else {
			panic("unsupported codec version")
		}
	}
	return
}

func EncodeRedoLog(r *model.RedoLog, en *msgp.Writer) (err error) {
	preMarshal(r)
	if _, err = en.Write(versionPrefix[:]); err != nil {
		return
	}
	versionField := binary.BigEndian.AppendUint16(nil, latestVersion)
	if _, err = en.Write(versionField); err != nil {
		return
	}
	return r.EncodeMsg(en)
}

func UnmarshalRedoLog(bts []byte) (r *model.RedoLog, o []byte, err error) {
	if len(bts) < versionPrefixLength {
		err = msgp.ErrShortBytes
		return
	}

	shouldBeV1 := false
	for i := 0; i < versionPrefixLength; i++ {
		if bts[i] != versionPrefix[i] {
			shouldBeV1 = true
			break
		}
	}
	if shouldBeV1 {
		var rv1 *codecv1.RedoLog = new(codecv1.RedoLog)
		if o, err = rv1.UnmarshalMsg(bts); err != nil {
			return
		}
		codecv1.PostUnmarshal(rv1)
		r = redoLogFromV1(rv1)
	} else {
		bts = bts[versionPrefixLength:]
		version, bts := decodeVersion(bts)
		if version == latestVersion {
			r = new(model.RedoLog)
			if o, err = r.UnmarshalMsg(bts); err != nil {
				return
			}
			postUnmarshal(r)
		} else {
			panic("unsupported codec version")
		}
	}
	return
}

func MarshalRedoLog(r *model.RedoLog, b []byte) (o []byte, err error) {
	preMarshal(r)
	b = append(b, versionPrefix[:]...)
	b = binary.BigEndian.AppendUint16(b, latestVersion)
	o, err = r.MarshalMsg(b)
	return
}

func decodeVersion(bts []byte) (uint16, []byte) {
	version := binary.BigEndian.Uint16(bts[0:versionFieldLength])
	return version, bts[versionFieldLength:]
}

func redoLogFromV1(rv1 *codecv1.RedoLog) (r *model.RedoLog) {
	r = &model.RedoLog{Type: (model.RedoLogType)(rv1.Type)}
	if rv1.RedoRow != nil && rv1.RedoRow.Row != nil {
		r.RedoRow.Row = &model.RowChangedEvent{
			StartTs:             rv1.RedoRow.Row.StartTs,
			CommitTs:            rv1.RedoRow.Row.CommitTs,
			RowID:               rv1.RedoRow.Row.RowID,
			Table:               tableNameFromV1(rv1.RedoRow.Row.Table),
			ColInfos:            rv1.RedoRow.Row.ColInfos,
			TableInfo:           rv1.RedoRow.Row.TableInfo,
			Columns:             make([]*model.Column, 0, len(rv1.RedoRow.Row.Columns)),
			PreColumns:          make([]*model.Column, 0, len(rv1.RedoRow.Row.PreColumns)),
			IndexColumns:        rv1.RedoRow.Row.IndexColumns,
			ApproximateDataSize: rv1.RedoRow.Row.ApproximateDataSize,
			SplitTxn:            rv1.RedoRow.Row.SplitTxn,
			ReplicatingTs:       rv1.RedoRow.Row.ReplicatingTs,
		}
		for _, c := range rv1.RedoRow.Row.Columns {
			r.RedoRow.Row.Columns = append(r.RedoRow.Row.Columns, columnFromV1(c))
		}
		for _, c := range rv1.RedoRow.Row.PreColumns {
			r.RedoRow.Row.PreColumns = append(r.RedoRow.Row.PreColumns, columnFromV1(c))
		}
	}
	if rv1.RedoDDL != nil && rv1.RedoDDL.DDL != nil {
		r.RedoDDL.DDL = &model.DDLEvent{
			StartTs:      rv1.RedoDDL.DDL.StartTs,
			CommitTs:     rv1.RedoDDL.DDL.CommitTs,
			Query:        rv1.RedoDDL.DDL.Query,
			TableInfo:    rv1.RedoDDL.DDL.TableInfo,
			PreTableInfo: rv1.RedoDDL.DDL.PreTableInfo,
			Type:         rv1.RedoDDL.DDL.Type,
			Done:         rv1.RedoDDL.DDL.Done,
		}
	}
	return
}

func tableNameFromV1(t *codecv1.TableName) *model.TableName {
	return &model.TableName{
		Schema:      t.Schema,
		Table:       t.Table,
		TableID:     t.TableID,
		IsPartition: t.IsPartition,
	}
}

func columnFromV1(c *codecv1.Column) *model.Column {
	return &model.Column{
		Name:             c.Name,
		Type:             c.Type,
		Charset:          c.Charset,
		Flag:             c.Flag,
		Value:            c.Value,
		Default:          c.Default,
		ApproximateBytes: c.ApproximateBytes,
	}
}

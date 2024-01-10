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
	"encoding/binary"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
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
// This makes it hard to extend in the future.
// However, in the old format (i.e. v1 format), the first 5 bytes are always same, which can be
// confirmed in v1/codec_gen.go. So we reuse those bytes, and add a version field in them.
var (
	versionPrefix = [versionPrefixLength]byte{0xff, 0xff}
)

func postUnmarshal(r *model.RedoLog) {
	workaroundColumn := func(c *model.Column, redoC *model.RedoColumn) {
		c.Flag = model.ColumnFlagType(redoC.Flag)
		if redoC.ValueIsEmptyBytes {
			c.Value = []byte{}
		} else {
			c.Value = redoC.Value
		}
	}

	if r.RedoRow.Row != nil {
		row := r.RedoRow.Row
		for i, c := range row.Columns {
			if c != nil {
				workaroundColumn(c, &r.RedoRow.Columns[i])
			}
		}
		for i, c := range row.PreColumns {
			if c != nil {
				workaroundColumn(c, &r.RedoRow.PreColumns[i])
			}
		}
		r.RedoRow.Columns = nil
		r.RedoRow.PreColumns = nil
	}
	if r.RedoDDL.DDL != nil {
		r.RedoDDL.DDL.Type = timodel.ActionType(r.RedoDDL.Type)
		r.RedoDDL.DDL.TableInfo = &model.TableInfo{
			TableName: r.RedoDDL.TableName,
		}
	}
}

func preMarshal(r *model.RedoLog) {
	// Workaround empty byte slice for msgp#247
	workaroundColumn := func(redoC *model.RedoColumn) {
		switch v := redoC.Value.(type) {
		case []byte:
			if len(v) == 0 {
				redoC.ValueIsEmptyBytes = true
			}
		}
	}

	if r.RedoRow.Row != nil {
		row := r.RedoRow.Row
		r.RedoRow.Columns = make([]model.RedoColumn, 0, len(row.Columns))
		r.RedoRow.PreColumns = make([]model.RedoColumn, 0, len(row.PreColumns))
		for _, c := range row.Columns {
			redoC := model.RedoColumn{}
			if c != nil {
				redoC.Value = c.Value
				redoC.Flag = uint64(c.Flag)
				workaroundColumn(&redoC)
			}
			r.RedoRow.Columns = append(r.RedoRow.Columns, redoC)
		}
		for _, c := range row.PreColumns {
			redoC := model.RedoColumn{}
			if c != nil {
				redoC.Value = c.Value
				redoC.Flag = uint64(c.Flag)
				workaroundColumn(&redoC)
			}
			r.RedoRow.PreColumns = append(r.RedoRow.PreColumns, redoC)
		}
	}
	if r.RedoDDL.DDL != nil {
		r.RedoDDL.Type = byte(r.RedoDDL.DDL.Type)
		if r.RedoDDL.DDL.TableInfo != nil {
			r.RedoDDL.TableName = r.RedoDDL.DDL.TableInfo.TableName
		}
	}
}

// UnmarshalRedoLog unmarshals a RedoLog from the given byte slice.
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

// MarshalRedoLog marshals a RedoLog into bytes.
func MarshalRedoLog(r *model.RedoLog, b []byte) (o []byte, err error) {
	preMarshal(r)
	b = append(b, versionPrefix[:]...)
	b = binary.BigEndian.AppendUint16(b, latestVersion)
	o, err = r.MarshalMsg(b)
	return
}

// MarshalRowAsRedoLog converts a RowChangedEvent into RedoLog, and then marshals it.
func MarshalRowAsRedoLog(r *model.RowChangedEvent, b []byte) (o []byte, err error) {
	log := &model.RedoLog{
		RedoRow: model.RedoRowChangedEvent{Row: r},
		Type:    model.RedoLogTypeRow,
	}
	return MarshalRedoLog(log, b)
}

// MarshalDDLAsRedoLog converts a DDLEvent into RedoLog, and then marshals it.
func MarshalDDLAsRedoLog(d *model.DDLEvent, b []byte) (o []byte, err error) {
	log := &model.RedoLog{
		RedoDDL: model.RedoDDLEvent{DDL: d},
		Type:    model.RedoLogTypeDDL,
	}
	return MarshalRedoLog(log, b)
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
		}
		r.RedoDDL.DDL.Done.Store(rv1.RedoDDL.DDL.Done)
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

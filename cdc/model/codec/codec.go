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

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	codecv1 "github.com/pingcap/tiflow/cdc/model/codec/v1"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/proto/redo"
	"github.com/tinylib/msgp/msgp"
)

const (
	v1HeaderLength      int = 4
	versionPrefixLength int = 2
	versionFieldLength  int = 2

	msgpLatestVersion uint16 = 3
	pbVersion         uint16 = 4
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
		if version == msgpLatestVersion {
			r = new(model.RedoLog)
			if o, err = r.UnmarshalMsg(bts); err != nil {
				return
			}
			postUnmarshal(r)
		} else if version == pbVersion {
			pbr := new(redo.RedoLog)
			if err = pbr.Unmarshal(bts); err != nil {
				return
			}
			r = new(model.RedoLog)
			if pbr.Type == redo.Type_DDL {
				r.Type = model.RedoLogTypeDDL
				r.RedoDDL = model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						StartTs:  pbr.Ddl.StartTs,
						CommitTs: pbr.Ddl.CommitTs,
						Query:    pbr.Ddl.Query,
					},
					Type: byte(pbr.Ddl.Type),
					TableName: model.TableName{
						Schema:      pbr.Ddl.TableName.Schema,
						Table:       pbr.Ddl.TableName.Table,
						TableID:     pbr.Ddl.TableName.TableId,
						IsPartition: pbr.Ddl.TableName.IsPartition,
					},
				}
			} else {
				r.Type = model.RedoLogTypeRow

				colsFunc := func(redoCols []*redo.RedoColumn) []*model.Column {
					if len(redoCols) == 0 {
						return nil
					}
					cols := make([]*model.Column, 0, len(redoCols))
					for _, c := range redoCols {
						cols = append(cols, &model.Column{
							Name:    c.Name,
							Type:    byte(c.Type),
							Charset: c.Charset,
							Flag:    model.ColumnFlagType(c.Flag),
						})
					}
					return cols
				}
				idxIds := make([][]int, 0, len(pbr.Row.Row.IndexIds))
				for _, ids := range pbr.Row.Row.IndexIds {
					idx := make([]int, 0, len(ids.Value))
					for _, id := range ids.Value {
						idx = append(idx, int(id))
					}
					idxIds = append(idxIds, idx)
				}

				colsValueFunx := func(redoCols []*redo.RedoColumn) []model.RedoColumn {
					if len(redoCols) == 0 {
						return nil
					}
					cols := make([]model.RedoColumn, 0, len(redoCols))
					for _, c := range redoCols {
						value, _, _ := msgp.ReadIntfBytes(c.Value)
						cols = append(cols, model.RedoColumn{
							Value:             value,
							ValueIsEmptyBytes: c.ValueIsEmptyBytes,
							Flag:              c.Flag,
						})
					}
					return cols
				}

				var tableInfo *model.TableName
				if pbr.Row.Row.Table != nil {
					tableInfo = &model.TableName{
						Schema:      pbr.Row.Row.Table.Schema,
						Table:       pbr.Row.Row.Table.Table,
						TableID:     pbr.Row.Row.Table.TableId,
						IsPartition: pbr.Row.Row.Table.IsPartition,
					}
				}
				r.RedoRow = model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						StartTs:      pbr.Row.Row.StartTs,
						CommitTs:     pbr.Row.Row.CommitTs,
						Table:        tableInfo,
						Columns:      colsFunc(pbr.Row.Columns),
						PreColumns:   colsFunc(pbr.Row.PreColumns),
						IndexColumns: idxIds,
					},
					Columns:    colsValueFunx(pbr.Row.Columns),
					PreColumns: colsValueFunx(pbr.Row.PreColumns),
				}
			}
			postUnmarshal(r)
		} else {
			panic("unsupported codec version")
		}
	}
	return
}

// MarshalRedoLog marshals a RedoLog into bytes.
func MarshalRedoLog(r *model.RedoLog) ([]byte, error) {
	preMarshal(r)
	colsCovertFunc := func(rawCols []*model.Column) ([]*redo.RedoColumn, error) {
		cols := make([]*redo.RedoColumn, len(rawCols))
		for i, col := range rawCols {
			if col == nil {
				continue
			}
			v, err := msgp.AppendIntf(nil, col.Value)
			if err != nil {
				return nil, errors.Trace(err)
			}
			valueIsEmptyBytes := false
			switch v := col.Value.(type) {
			case []byte:
				if len(v) == 0 {
					valueIsEmptyBytes = true
				}
			}
			cols[i] = &redo.RedoColumn{
				Name:              col.Name,
				Type:              int32(col.Type),
				Charset:           col.Charset,
				Collation:         col.Collation,
				Flag:              uint64(col.Flag),
				Value:             v,
				ValueIsEmptyBytes: valueIsEmptyBytes,
			}
		}
		return cols, nil
	}
	pbRedoLog := &redo.RedoLog{}
	if r.Type == model.RedoLogTypeRow {
		pbRedoLog.Type = redo.Type_DML
		index := make([]*redo.IntArr, len(r.RedoRow.Row.IndexColumns))
		for i, idx := range r.RedoRow.Row.IndexColumns {
			int32Arr := make([]int32, len(idx))
			for j, id := range idx {
				int32Arr[j] = int32(id)
			}
			index[i] = &redo.IntArr{
				Value: int32Arr,
			}
		}
		preCols, err := colsCovertFunc(r.RedoRow.Row.PreColumns)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cols, err := colsCovertFunc(r.RedoRow.Row.Columns)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var tableInfo *redo.TableName
		if r.RedoRow.Row.Table != nil {
			tableInfo = &redo.TableName{
				Schema:      r.RedoRow.Row.Table.Schema,
				Table:       r.RedoRow.Row.Table.Table,
				TableId:     r.RedoRow.Row.Table.TableID,
				IsPartition: r.RedoRow.Row.Table.IsPartition,
			}
		}
		pbRedoLog.Row = &redo.RedoRowChangedEvent{
			Row: &redo.RowChangedEvent{
				CommitTs: r.RedoRow.Row.CommitTs,
				StartTs:  r.RedoRow.Row.StartTs,
				Table:    tableInfo,
				IndexIds: index,
			},
			PreColumns: preCols,
			Columns:    cols,
		}
	} else {
		pbRedoLog.Type = redo.Type_DDL
		pbRedoLog.Ddl = &redo.RedoDDLEvent{
			Type:     int32(r.RedoDDL.Type),
			CommitTs: r.RedoDDL.DDL.CommitTs,
			StartTs:  r.RedoDDL.DDL.StartTs,
			Query:    r.RedoDDL.DDL.Query,
			TableName: &redo.TableName{
				Schema:      r.RedoDDL.TableName.Schema,
				Table:       r.RedoDDL.TableName.Table,
				TableId:     r.RedoDDL.TableName.TableID,
				IsPartition: r.RedoDDL.TableName.IsPartition,
			},
		}
	}
	o, err := pbRedoLog.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	var b []byte
	b = append(b, versionPrefix[:]...)
	b = binary.BigEndian.AppendUint16(b, pbVersion)
	return append(b, o...), nil
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

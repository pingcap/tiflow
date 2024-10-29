// Copyright 2022 PingCAP, Inc.
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

package internal

import (
	"encoding/base64"
	"encoding/json"
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// Column is a type only used in codec internally.
type Column struct {
	Type byte `json:"t"`
	// Deprecated: please use Flag instead.
	WhereHandle bool                 `json:"h,omitempty"`
	Flag        model.ColumnFlagType `json:"f"`
	Value       any                  `json:"v"`
}

// FromRowChangeColumn converts from a row changed column to a codec column.
func (c *Column) FromRowChangeColumn(col model.ColumnDataX) {
	c.Type = col.GetType()
	c.Flag = col.GetFlag()
	if c.Flag.IsHandleKey() {
		c.WhereHandle = true
	}
	if col.Value == nil {
		c.Value = nil
		return
	}
	switch c.Type {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		var str string
		switch col.Value.(type) {
		case []byte:
			str = string(col.Value.([]byte))
		case string:
			str = col.Value.(string)
		default:
			log.Panic("invalid column value, please report a bug", zap.Any("col", col))
		}
		if c.Flag.IsBinary() {
			str = strconv.Quote(str)
			str = str[1 : len(str)-1]
		}
		c.Value = str
	case mysql.TypeTiDBVectorFloat32:
		c.Value = col.Value.(types.VectorFloat32).String()
	default:
		c.Value = col.Value
	}
}

// ToRowChangeColumn converts from a codec column to a row changed column.
func (c *Column) ToRowChangeColumn(name string) *model.Column {
	col := new(model.Column)
	col.Type = c.Type
	col.Flag = c.Flag
	col.Name = name
	col.Value = c.Value
	if c.Value == nil {
		return col
	}
	switch col.Type {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		str := col.Value.(string)
		var err error
		if c.Flag.IsBinary() {
			str, err = strconv.Unquote("\"" + str + "\"")
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		}
		col.Value = []byte(str)
	case mysql.TypeFloat:
		col.Value = float32(col.Value.(float64))
	case mysql.TypeYear:
		col.Value = int64(col.Value.(uint64))
	case mysql.TypeEnum, mysql.TypeSet:
		val, err := col.Value.(json.Number).Int64()
		if err != nil {
			log.Panic("invalid column value for enum, please report a bug",
				zap.Any("col", c), zap.Error(err))
		}
		col.Value = uint64(val)
	case mysql.TypeTiDBVectorFloat32:
	default:
	}
	return col
}

// FormatColumn formats a codec column.
func FormatColumn(c Column) Column {
	switch c.Type {
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob,
		mysql.TypeLongBlob, mysql.TypeBlob:
		if s, ok := c.Value.(string); ok {
			var err error
			c.Value, err = base64.StdEncoding.DecodeString(s)
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		if s, ok := c.Value.(json.Number); ok {
			f64, err := s.Float64()
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
			c.Value = f64
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		if s, ok := c.Value.(json.Number); ok {
			var err error
			if c.Flag.IsUnsigned() {
				c.Value, err = strconv.ParseUint(s.String(), 10, 64)
			} else {
				c.Value, err = strconv.ParseInt(s.String(), 10, 64)
			}
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		} else if f, ok := c.Value.(float64); ok {
			if c.Flag.IsUnsigned() {
				c.Value = uint64(f)
			} else {
				c.Value = int64(f)
			}
		}
	case mysql.TypeBit:
		if s, ok := c.Value.(json.Number); ok {
			intNum, err := s.Int64()
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
			c.Value = uint64(intNum)
		}
	}
	return c
}

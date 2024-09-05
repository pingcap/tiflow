// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"encoding/json"
	"log"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

// CheckVectorIndexForColumnModify checks Vector Index constraints for MODIFY COLUMN.
// func CheckVectorIndexForColumnModify(oldCol *table.Column, newCol *table.Column) error {
// 	if oldCol.VectorIndex == nil && newCol.VectorIndex == nil {
// 		return nil
// 	}
// 	if oldCol.VectorIndex == nil && newCol.VectorIndex != nil {
// 		return errors.Errorf("currently HNSW index can be only defined when creating the table")
// 	}
// 	if oldCol.VectorIndex != nil && newCol.VectorIndex == nil {
// 		return errors.Errorf("currently HNSW index can not be removed")
// 	}
// 	if oldCol.FieldType.GetFlen() != newCol.FieldType.GetFlen() {
// 		return errors.New("cannot modify vector column's dimention when HNSW index is defined")
// 	}
// 	if oldCol.FieldType.GetType() != newCol.FieldType.GetType() {
// 		return errors.New("cannot modify column data type when HNSW index is defined")
// 	}
// 	if *(oldCol.VectorIndex) != *(newCol.VectorIndex) {
// 		return errors.New("currently HNSW index cannot be modified")
// 	}
// 	return nil
// }

func ParseVectorFromElement(values []float32) (types.VectorFloat32, error) {
	dim := len(values)
	if err := types.CheckVectorDimValid(dim); err != nil {
		return types.ZeroVectorFloat32, err
	}
	vec := types.InitVectorFloat32(dim)
	copy(vec.Elements(), values)
	return vec, nil
}

func VectorElement2String(elements []interface{}) string {
	buf := make([]byte, 0, 2+len(elements)*2)
	buf = append(buf, '[')
	for i, val := range elements {
		if i > 0 {
			buf = append(buf, ',')
		}
		switch v := val.(type) {
		case json.Number:
			num, err := v.Float64()
			if err != nil {
				log.Panic("failed to decode val", zap.Any("val", val), zap.Error(err))
			}
			buf = strconv.AppendFloat(buf, num, 'f', -1, 32)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'f', -1, 32)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'f', -1, 32)
		default:
			log.Panic("failed to decode val type", zap.Any("val", val), zap.Any("type", reflect.TypeOf(v)))
		}
	}
	buf = append(buf, ']')
	// buf is not used elsewhere, so it's safe to just cast to String
	return unsafe.String(unsafe.SliceData(buf), len(buf))
}

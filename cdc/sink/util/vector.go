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

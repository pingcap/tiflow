// Copyright 2021 PingCAP, Inc.
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

package chunk

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"go.uber.org/zap"
)

const (
	lt  = "<"
	lte = "<="
	gt  = ">"
)

// Type is the type of the chunk
type Type int

// List all chunk types
const (
	Bucket Type = iota + 1
	Random
	Limit
	Others
	Empty
)

// Bound represents a bound for a column
type Bound struct {
	Column string `json:"column"`
	Lower  string `json:"lower"`
	Upper  string `json:"upper"`

	HasLower bool `json:"has-lower"`
	HasUpper bool `json:"has-upper"`
}

// CID is to identify the sequence of chunks
type CID struct {
	TableIndex int `json:"table-index"`
	// we especially treat random split has only one bucket
	// which is the whole table
	// range is [left, right]
	BucketIndexLeft  int `json:"bucket-index-left"`
	BucketIndexRight int `json:"bucket-index-right"`
	ChunkIndex       int `json:"chunk-index"`
	//  `ChunkCnt` is the number of chunks in this bucket
	//  We can compare `ChunkIndex` and `ChunkCnt` to know
	// whether this chunk is the last one
	ChunkCnt int `json:"chunk-count"`
}

// GetInitCID return an empty CID
func GetInitCID() *CID {
	return &CID{
		TableIndex:       -1,
		BucketIndexLeft:  -1,
		BucketIndexRight: -1,
		ChunkIndex:       -1,
		ChunkCnt:         0,
	}
}

// Compare compare two CIDs
func (c *CID) Compare(o *CID) int {
	if c.TableIndex < o.TableIndex {
		return -1
	}
	if c.TableIndex > o.TableIndex {
		return 1
	}

	// c.TableIndex == o.TableIndex
	if c.BucketIndexLeft < o.BucketIndexLeft {
		return -1
	}
	if c.BucketIndexLeft > o.BucketIndexLeft {
		return 1
	}
	// c.BucketIndexLeft == o.BucketIndexLeft
	if c.ChunkIndex < o.ChunkIndex {
		return -1
	}
	if c.ChunkIndex == o.ChunkIndex {
		return 0
	}
	return 1
}

// Copy return a same CID
func (c *CID) Copy() *CID {
	cp := *c
	return &cp
}

// ToString return string for CID
func (c *CID) ToString() string {
	return fmt.Sprintf("%d:%d-%d:%d:%d", c.TableIndex, c.BucketIndexLeft, c.BucketIndexRight, c.ChunkIndex, c.ChunkCnt)
}

// FromString get CID from given string
func (c *CID) FromString(s string) error {
	ids := strings.Split(s, ":")
	tableIndex, err := strconv.Atoi(ids[0])
	if err != nil {
		return errors.Trace(err)
	}

	bucketIndex := strings.Split(ids[1], "-")
	bucketIndexLeft, err := strconv.Atoi(bucketIndex[0])
	if err != nil {
		return errors.Trace(err)
	}
	bucketIndexRight, err := strconv.Atoi(bucketIndex[1])
	if err != nil {
		return errors.Trace(err)
	}

	chunkIndex, err := strconv.Atoi(ids[2])
	if err != nil {
		return errors.Trace(err)
	}
	chunkCnt, err := strconv.Atoi(ids[3])
	if err != nil {
		return errors.Trace(err)
	}
	c.TableIndex, c.BucketIndexLeft, c.BucketIndexRight, c.ChunkIndex, c.ChunkCnt = tableIndex, bucketIndexLeft, bucketIndexRight, chunkIndex, chunkCnt
	return nil
}

// Range represents chunk range
type Range struct {
	Index   *CID     `json:"index"`
	Type    Type     `json:"type"`
	Bounds  []*Bound `json:"bounds"`
	IsFirst bool     `json:"is-first"`
	IsLast  bool     `json:"is-last"`

	Where string        `json:"where"`
	Args  []interface{} `json:"args"`

	columnOffset map[string]int
}

// IsFirstChunkForBucket return true if it's the first chunk
func (r *Range) IsFirstChunkForBucket() bool {
	return r.Index.ChunkIndex == 0
}

// IsLastChunkForBucket return true if it's the last chunk
func (r *Range) IsLastChunkForBucket() bool {
	return r.Index.ChunkIndex == r.Index.ChunkCnt-1
}

// NewChunkRange return a Range.
func NewChunkRange() *Range {
	return &Range{
		Bounds:       make([]*Bound, 0, 2),
		columnOffset: make(map[string]int),
		Index:        &CID{},
	}
}

// NewChunkRangeOffset return a Range in sequence
func NewChunkRangeOffset(columnOffset map[string]int) *Range {
	bounds := make([]*Bound, len(columnOffset))
	for column, offset := range columnOffset {
		bounds[offset] = &Bound{
			Column:   column,
			HasLower: false,
			HasUpper: false,
		}
	}
	return &Range{
		Bounds:       bounds,
		columnOffset: columnOffset,
	}
}

// IsLastChunkForTable return true if it's the last chunk
func (r *Range) IsLastChunkForTable() bool {
	if r.IsLast {
		return true
	}
	// calculate from bounds
	for _, b := range r.Bounds {
		if b.HasUpper {
			return false
		}
	}
	return true
}

// IsFirstChunkForTable return true if it's the first chunk
func (r *Range) IsFirstChunkForTable() bool {
	if r.IsFirst {
		return true
	}
	// calculate from bounds
	for _, b := range r.Bounds {
		if b.HasLower {
			return false
		}
	}
	return true
}

// String returns the string of Range, used for log.
func (r *Range) String() string {
	chunkBytes, err := json.Marshal(r)
	if err != nil {
		log.Warn("fail to encode chunk into string", zap.Error(err))
		return ""
	}

	return string(chunkBytes)
}

// ToString return string for range
func (r *Range) ToString(collation string) (string, []interface{}) {
	if collation != "" {
		collation = fmt.Sprintf(" COLLATE '%s'", collation)
	}

	/* for example:
	there is a bucket in TiDB, and the lowerbound and upperbound are (A, B1, C1), (A, B2, C2), and the columns are `a`, `b` and `c`,
	this bucket's data range is (a = A) AND (b > B1 or (b == B1 and c > C1)) AND (b < B2 or (b == B2 and c <= C2))
	*/

	sameCondition := make([]string, 0, 1)
	lowerCondition := make([]string, 0, 1)
	upperCondition := make([]string, 0, 1)
	sameArgs := make([]interface{}, 0, 1)
	lowerArgs := make([]interface{}, 0, 1)
	upperArgs := make([]interface{}, 0, 1)

	preConditionForLower := make([]string, 0, 1)
	preConditionForUpper := make([]string, 0, 1)
	preConditionArgsForLower := make([]interface{}, 0, 1)
	preConditionArgsForUpper := make([]interface{}, 0, 1)

	i := 0
	for ; i < len(r.Bounds); i++ {
		bound := r.Bounds[i]
		if !(bound.HasLower && bound.HasUpper) {
			break
		}

		if bound.Lower != bound.Upper {
			break
		}

		sameCondition = append(sameCondition, fmt.Sprintf("%s%s = ?", dbutil.ColumnName(bound.Column), collation))
		sameArgs = append(sameArgs, bound.Lower)
	}

	if i == len(r.Bounds) && i > 0 {
		// All the columns are equal in bounds, should return FALSE!
		return "FALSE", nil
	}

	for ; i < len(r.Bounds); i++ {
		bound := r.Bounds[i]
		lowerSymbol := gt
		upperSymbol := lt
		if i == len(r.Bounds)-1 {
			upperSymbol = lte
		}

		if bound.HasLower {
			if len(preConditionForLower) > 0 {
				lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s%s %s ?)", strings.Join(preConditionForLower, " AND "), dbutil.ColumnName(bound.Column), collation, lowerSymbol))
				lowerArgs = append(append(lowerArgs, preConditionArgsForLower...), bound.Lower)
			} else {
				lowerCondition = append(lowerCondition, fmt.Sprintf("(%s%s %s ?)", dbutil.ColumnName(bound.Column), collation, lowerSymbol))
				lowerArgs = append(lowerArgs, bound.Lower)
			}
			preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s%s = ?", dbutil.ColumnName(bound.Column), collation))
			preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
		}

		if bound.HasUpper {
			if len(preConditionForUpper) > 0 {
				upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s%s %s ?)", strings.Join(preConditionForUpper, " AND "), dbutil.ColumnName(bound.Column), collation, upperSymbol))
				upperArgs = append(append(upperArgs, preConditionArgsForUpper...), bound.Upper)
			} else {
				upperCondition = append(upperCondition, fmt.Sprintf("(%s%s %s ?)", dbutil.ColumnName(bound.Column), collation, upperSymbol))
				upperArgs = append(upperArgs, bound.Upper)
			}
			preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s%s = ?", dbutil.ColumnName(bound.Column), collation))
			preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
		}
	}

	if len(sameCondition) == 0 {
		if len(upperCondition) == 0 && len(lowerCondition) == 0 {
			return "TRUE", nil
		}

		if len(upperCondition) == 0 {
			return strings.Join(lowerCondition, " OR "), lowerArgs
		}

		if len(lowerCondition) == 0 {
			return strings.Join(upperCondition, " OR "), upperArgs
		}

		return fmt.Sprintf("(%s) AND (%s)", strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR ")), append(lowerArgs, upperArgs...)
	}

	if len(upperCondition) == 0 && len(lowerCondition) == 0 {
		return strings.Join(sameCondition, " AND "), sameArgs
	}

	if len(upperCondition) == 0 {
		return fmt.Sprintf("(%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(lowerCondition, " OR ")), append(sameArgs, lowerArgs...)
	}

	if len(lowerCondition) == 0 {
		return fmt.Sprintf("(%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(upperCondition, " OR ")), append(sameArgs, upperArgs...)
	}

	return fmt.Sprintf("(%s) AND (%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR ")), append(append(sameArgs, lowerArgs...), upperArgs...)
}

// ToMeta return string for range
func (r *Range) ToMeta() string {
	lowerCondition := make([]string, 0, 1)
	upperCondition := make([]string, 0, 1)
	columnName := make([]string, 0, 1)
	for _, bound := range r.Bounds {
		columnName = append(columnName, bound.Column)
		if bound.HasLower {
			lowerCondition = append(lowerCondition, bound.Lower)
		}
		if bound.HasUpper {
			upperCondition = append(upperCondition, bound.Upper)
		}
	}
	if len(upperCondition) == 0 && len(lowerCondition) == 0 {
		return "range in sequence: Full"
	}
	if len(upperCondition) == 0 {
		return fmt.Sprintf("range in sequence: (%s) < (%s)", strings.Join(lowerCondition, ","), strings.Join(columnName, ","))
	}
	if len(lowerCondition) == 0 {
		return fmt.Sprintf("range in sequence: (%s) <= (%s)", strings.Join(columnName, ","), strings.Join(upperCondition, ","))
	}
	return fmt.Sprintf("range in sequence: (%s) < (%s) <= (%s)", strings.Join(lowerCondition, ","), strings.Join(columnName, ","), strings.Join(upperCondition, ","))
}

func (r *Range) addBound(bound *Bound) {
	r.Bounds = append(r.Bounds, bound)
	r.columnOffset[bound.Column] = len(r.Bounds) - 1
}

// Update update the range
func (r *Range) Update(column, lower, upper string, updateLower, updateUpper bool) {
	if offset, ok := r.columnOffset[column]; ok {
		// update the bound
		if updateLower {
			r.Bounds[offset].Lower = lower
			r.Bounds[offset].HasLower = true
		}
		if updateUpper {
			r.Bounds[offset].Upper = upper
			r.Bounds[offset].HasUpper = true
		}

		return
	}

	// add a new bound
	r.addBound(&Bound{
		Column:   column,
		Lower:    lower,
		Upper:    upper,
		HasLower: updateLower,
		HasUpper: updateUpper,
	})
}

// Copy return a new range
func (r *Range) Copy() *Range {
	newChunk := NewChunkRange()
	for _, bound := range r.Bounds {
		newChunk.addBound(&Bound{
			Column:   bound.Column,
			Lower:    bound.Lower,
			Upper:    bound.Upper,
			HasLower: bound.HasLower,
			HasUpper: bound.HasUpper,
		})
	}

	return newChunk
}

// Clone return a new range
func (r *Range) Clone() *Range {
	newChunk := NewChunkRange()
	for _, bound := range r.Bounds {
		newChunk.addBound(&Bound{
			Column:   bound.Column,
			Lower:    bound.Lower,
			Upper:    bound.Upper,
			HasLower: bound.HasLower,
			HasUpper: bound.HasUpper,
		})
	}
	newChunk.Type = r.Type
	newChunk.Where = r.Where
	newChunk.Args = r.Args
	for i, v := range r.columnOffset {
		newChunk.columnOffset[i] = v
	}
	newChunk.Index = r.Index.Copy()
	newChunk.IsFirst = r.IsFirst
	newChunk.IsLast = r.IsLast
	return newChunk
}

// CopyAndUpdate update the range
func (r *Range) CopyAndUpdate(column, lower, upper string, updateLower, updateUpper bool) *Range {
	newChunk := r.Copy()
	newChunk.Update(column, lower, upper, updateLower, updateUpper)
	return newChunk
}

// InitChunks init the given chunks
// Notice: chunk may contain not only one bucket, which can be expressed as a range [3, 5],
//
//	And `lastBucketID` means the `5` and `firstBucketID` means the `3`.
func InitChunks(chunks []*Range, t Type, firstBucketID, lastBucketID int, index int, collation, limits string, chunkCnt int) {
	if chunks == nil {
		return
	}
	for _, chunk := range chunks {
		conditions, args := chunk.ToString(collation)
		chunk.Where = fmt.Sprintf("((%s) AND (%s))", conditions, limits)
		chunk.Args = args
		chunk.Index = &CID{
			BucketIndexLeft:  firstBucketID,
			BucketIndexRight: lastBucketID,
			ChunkIndex:       index,
			ChunkCnt:         chunkCnt,
		}
		chunk.Type = t
		index++
	}
}

// InitChunk initialize the chunk
func InitChunk(chunk *Range, t Type, firstBucketID, lastBucketID int, collation, limits string) {
	conditions, args := chunk.ToString(collation)
	chunk.Where = fmt.Sprintf("((%s) AND (%s))", conditions, limits)
	chunk.Args = args
	chunk.Index = &CID{
		BucketIndexLeft:  firstBucketID,
		BucketIndexRight: lastBucketID,
		ChunkIndex:       0,
		ChunkCnt:         1,
	}
	chunk.Type = t
}

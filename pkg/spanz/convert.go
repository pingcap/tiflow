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

package spanz

import (
	"fmt"
	"reflect"
	"sort"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"go.uber.org/zap"
)

// HexKey returns a hex string generated from the key.
func HexKey(key []byte) string {
	// TODO(qupeng): improve the function.
	str := ""
	for _, c := range key {
		str += fmt.Sprintf("%02X", c)
	}
	return str
}

// ArrayToSpan converts an array of TableID to an array of Span.
func ArrayToSpan(in []tablepb.TableID) []tablepb.Span {
	out := make([]tablepb.Span, 0, len(in))
	for _, tableID := range in {
		out = append(out, tablepb.Span{TableID: tableID})
	}
	return out
}

// TableIDToComparableSpan converts a TableID to a Span whose
// StartKey and EndKey are encoded in Comparable format.
func TableIDToComparableSpan(tableID tablepb.TableID) tablepb.Span {
	startKey, endKey := GetTableRange(tableID)
	return tablepb.Span{
		TableID:  tableID,
		StartKey: ToComparableKey(startKey),
		EndKey:   ToComparableKey(endKey),
	}
}

// TableIDToComparableRange returns a range of a table,
// start and end are encoded in Comparable format.
func TableIDToComparableRange(tableID tablepb.TableID) (start, end tablepb.Span) {
	tableSpan := TableIDToComparableSpan(tableID)
	start = tableSpan
	start.EndKey = nil
	end = tableSpan
	end.StartKey = tableSpan.EndKey
	end.EndKey = nil
	return
}

type sortableSpans []tablepb.Span

func (a sortableSpans) Len() int           { return len(a) }
func (a sortableSpans) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortableSpans) Less(i, j int) bool { return a[i].Less(&a[j]) }

// Sort sorts a slice of Span.
func Sort(spans []tablepb.Span) {
	sort.Sort(sortableSpans(spans))
}

// hashableSpan is a hashable span, which can be used as a map key.
type hashableSpan struct {
	TableID  tablepb.TableID
	StartKey string
	EndKey   string
}

// toHashableSpan converts a Span to a hashable span.
func toHashableSpan(span tablepb.Span) hashableSpan {
	return hashableSpan{
		TableID:  span.TableID,
		StartKey: unsafeBytesToString(span.StartKey),
		EndKey:   unsafeBytesToString(span.EndKey),
	}
}

// toSpan converts to Span.
func (h hashableSpan) toSpan() tablepb.Span {
	return tablepb.Span{
		TableID:  h.TableID,
		StartKey: unsafeStringToBytes(h.StartKey),
		EndKey:   unsafeStringToBytes(h.EndKey),
	}
}

// unsafeStringToBytes converts string to byte without memory allocation.
// The []byte must not be mutated.
// See: https://cs.opensource.google/go/go/+/refs/tags/go1.19.4:src/strings/builder.go;l=48
func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// unsafeStringToBytes converts string to byte without memory allocation.
// The returned []byte must not be mutated.
// See: https://groups.google.com/g/golang-nuts/c/Zsfk-VMd_fU/m/O1ru4fO-BgAJ
func unsafeStringToBytes(s string) []byte {
	if len(s) == 0 {
		return []byte{}
	}
	const maxCap = 0x7fff0000
	if len(s) > maxCap {
		log.Panic("string is too large", zap.Int("len", len(s)))
	}
	return (*[maxCap]byte)(unsafe.Pointer(
		(*reflect.StringHeader)(unsafe.Pointer(&s)).Data),
	)[:len(s):len(s)]
}

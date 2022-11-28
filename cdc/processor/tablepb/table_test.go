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

package tablepb

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func decode(s string) []byte {
	b, _ := hex.DecodeString(s)
	return b
}

func TestSpanString(t *testing.T) {
	t.Parallel()

	span := Span{
		TableID:  555555,
		StartKey: decode("7480000000000009ff89000000f8"),
		EndKey:   decode("748000000000000dffaa5f6980ff"),
	}
	require.Equal(t, strings.TrimSpace(`
{table_id:555555,start_key:7480000000000009ff89000000f8,end_key:748000000000000dffaa5f6980ff}
`), span.String())

	span = Span{
		TableID: 555555,
	}
	require.Equal(t, `{table_id:555555}`, span.String())
}

func TestSpanJSON(t *testing.T) {
	t.Parallel()

	span := Span{
		TableID:  555555,
		StartKey: decode("7480000000000009ff89000000f8"),
		EndKey:   decode("748000000000000dffaa5f6980ff"),
	}
	js, _ := json.Marshal(span)
	require.Equal(t, strings.TrimSpace(`
"{table_id:555555,start_key:7480000000000009ff89000000f8,end_key:748000000000000dffaa5f6980ff}"
`), string(js))

	span = Span{
		TableID: 555555,
	}
	js, _ = json.Marshal(span)
	require.Equal(t, `"{table_id:555555}"`, string(js))
}

func TestSpanProtoMarshalText(t *testing.T) {
	t.Parallel()

	span := Span{
		TableID:  555555,
		StartKey: decode("7480000000000009ff89000000f8"),
		EndKey:   decode("748000000000000dffaa5f6980ff"),
	}
	s := proto.CompactTextString(&span)
	require.Equal(t, strings.TrimSpace(`
{table_id:555555,start_key:7480000000000009ff89000000f8,end_key:748000000000000dffaa5f6980ff}
`), s)
}

func TestSpanLess(t *testing.T) {
	t.Parallel()

	a1 := &Span{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	a2 := &Span{TableID: 1, StartKey: []byte("c"), EndKey: []byte("d")}
	b := &Span{TableID: 2, StartKey: []byte("e"), EndKey: []byte("f")}
	require.True(t, a1.Less(a2))
	require.True(t, a1.Less(b))
	require.True(t, a2.Less(b))
}

func TestSpanEq(t *testing.T) {
	t.Parallel()

	a := &Span{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	b := &Span{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	require.True(t, a.Eq(a))
	require.True(t, a.Eq(b))
	require.True(t, b.Eq(a))

	c := &Span{TableID: 1, StartKey: []byte("a"), EndKey: []byte("c")}
	require.False(t, a.Eq(c))
	d := &Span{TableID: 2, StartKey: []byte("a"), EndKey: []byte("b")}
	require.False(t, a.Eq(d))
	require.True(t, d.Eq(d))
}

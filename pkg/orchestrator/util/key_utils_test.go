// Copyright 2020 PingCAP, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEtcdKey(t *testing.T) {
	key := NewEtcdKey("/a/b/c")
	require.Equal(t, NewEtcdKeyFromBytes([]byte("/a/b/c")), key)
	require.Equal(t, "/a/b/c", key.String())
	require.Equal(t, []byte("/a/b/c"), key.Bytes())
	require.Equal(t, "/a", key.Head().String())
	require.Equal(t, "/b/c", key.Tail().String())
	require.Equal(t, "/c", key.RemovePrefix(&EtcdPrefix{"/a/b"}).String())
	require.Equal(t, "/a/b/c", key.AsRelKey().String())
}

func TestEtcdRelKey(t *testing.T) {
	key := NewEtcdRelKey("/a/b/c")
	require.Equal(t, NewEtcdRelKeyFromBytes([]byte("/a/b/c")), key)
	require.Equal(t, "/a/b/c", key.String())
	require.Equal(t, []byte("/a/b/c"), key.Bytes())
	require.Equal(t, "/a", key.Head().String())
	require.Equal(t, "/b/c", key.Tail().String())
	require.Equal(t, "/c", key.RemovePrefix(&EtcdRelPrefix{EtcdPrefix{"/a/b"}}).String())
	require.Equal(t, "/a/b/c", key.AsPrefix().String())
}

func TestEtcdPrefix(t *testing.T) {
	prefix := NewEtcdPrefix("/aa/bb/cc")
	require.Equal(t, NewEtcdPrefixFromBytes([]byte("/aa/bb/cc")), prefix)
	require.Equal(t, "/aa/bb/cc", prefix.String())
	require.Equal(t, []byte("/aa/bb/cc"), prefix.Bytes())
	require.Equal(t, "/bb/cc", prefix.Tail().String())
	require.Equal(t, "/aa", prefix.Head().String())
	require.Equal(t, "/aa/bb/cc/dd", prefix.FullKey(NewEtcdRelKey("/dd")).String())
}

func TestEtcdRelPrefix(t *testing.T) {
	prefix := NewEtcdRelPrefix("/aa/bb/cc")
	require.Equal(t, NewEtcdRelPrefixFromBytes([]byte("/aa/bb/cc")), prefix)
	require.Equal(t, "/aa/bb/cc", prefix.String())
	require.Equal(t, []byte("/aa/bb/cc"), prefix.Bytes())
	require.Equal(t, "/bb/cc", prefix.Tail().String())
	require.Equal(t, "/aa", prefix.Head().String())
}

func TestNormalizePrefix(t *testing.T) {
	require.Equal(t, NewEtcdPrefix("/aaa"), NormalizePrefix("aaa"))
	require.Equal(t, NewEtcdPrefix("/aaa"), NormalizePrefix("aaa/"))
	require.Equal(t, NewEtcdPrefix("/aaa"), NormalizePrefix("/aaa"))
	require.Equal(t, NewEtcdPrefix("/aaa"), NormalizePrefix("/aaa/"))
}

func TestCornerCases(t *testing.T) {
	require.Panics(t, func() { NewEtcdPrefix("").Head() }, "Empty EtcdPrefix")
	require.Panics(t, func() { NewEtcdPrefix("").Tail() }, "Empty EtcdPrefix")
	require.Equal(t, NewEtcdPrefix(""), NewEtcdPrefix("aaa").Head())
	require.Equal(t, NewEtcdRelPrefix("aaa"), NewEtcdPrefix("aaa").Tail())

	require.Panics(t, func() { NewEtcdKey("").Head() }, "Empty EtcdKey")
	require.Panics(t, func() { NewEtcdKey("").Tail() }, "Empty EtcdKey")
	require.Equal(t, NewEtcdPrefix(""), NewEtcdKey("aaa").Head())
	require.Equal(t, NewEtcdRelKey("aaa"), NewEtcdKey("aaa").Tail())
}

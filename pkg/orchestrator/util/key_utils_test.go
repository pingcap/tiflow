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
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func TestEtcdKey(t *testing.T) {
	defer testleak.AfterTest(t)()
	key := NewEtcdKey("/a/b/c")
	require.Equal(t, key, NewEtcdKeyFromBytes([]byte("/a/b/c")))
	require.Equal(t, key.String(), "/a/b/c")
	require.Equal(t, key.Bytes(), []byte("/a/b/c"))
	require.Equal(t, key.Head().String(), "/a")
	require.Equal(t, key.Tail().String(), "/b/c")
	require.Equal(t, key.RemovePrefix(&EtcdPrefix{"/a/b"}).String(), "/c")
	require.Equal(t, key.AsRelKey().String(), "/a/b/c")
}

func TestEtcdRelKey(t *testing.T) {
	defer testleak.AfterTest(t)()
	key := NewEtcdRelKey("/a/b/c")
	require.Equal(t, key, NewEtcdRelKeyFromBytes([]byte("/a/b/c")))
	require.Equal(t, key.String(), "/a/b/c")
	require.Equal(t, key.Bytes(), []byte("/a/b/c"))
	require.Equal(t, key.Head().String(), "/a")
	require.Equal(t, key.Tail().String(), "/b/c")
	require.Equal(t, key.RemovePrefix(&EtcdRelPrefix{EtcdPrefix{"/a/b"}}).String(), "/c")
	require.Equal(t, key.AsPrefix().String(), "/a/b/c")
}

func TestEtcdPrefix(t *testing.T) {
	defer testleak.AfterTest(t)()
	prefix := NewEtcdPrefix("/aa/bb/cc")
	require.Equal(t, prefix, NewEtcdPrefixFromBytes([]byte("/aa/bb/cc")))
	require.Equal(t, prefix.String(), "/aa/bb/cc")
	require.Equal(t, prefix.Bytes(), []byte("/aa/bb/cc"))
	require.Equal(t, prefix.Tail().String(), "/bb/cc")
	require.Equal(t, prefix.Head().String(), "/aa")
	require.Equal(t, prefix.FullKey(NewEtcdRelKey("/dd")).String(), "/aa/bb/cc/dd")
}

func TestEtcdRelPrefix(t *testing.T) {
	defer testleak.AfterTest(t)()
	prefix := NewEtcdRelPrefix("/aa/bb/cc")
	require.Equal(t, prefix, NewEtcdRelPrefixFromBytes([]byte("/aa/bb/cc")))
	require.Equal(t, prefix.String(), "/aa/bb/cc")
	require.Equal(t, prefix.Bytes(), []byte("/aa/bb/cc"))
	require.Equal(t, prefix.Tail().String(), "/bb/cc")
	require.Equal(t, prefix.Head().String(), "/aa")
}

func TestNormalizePrefix(t *testing.T) {
	defer testleak.AfterTest(t)()
	require.Equal(t, NormalizePrefix("aaa"), NewEtcdPrefix("/aaa"))
	require.Equal(t, NormalizePrefix("aaa/"), NewEtcdPrefix("/aaa"))
	require.Equal(t, NormalizePrefix("/aaa"), NewEtcdPrefix("/aaa"))
	require.Equal(t, NormalizePrefix("/aaa/"), NewEtcdPrefix("/aaa"))
}

func TestCornerCases(t *testing.T) {
	defer testleak.AfterTest(t)()
	require.Panics(t, func() { NewEtcdPrefix("").Head() }, "Empty EtcdPrefix")
	require.Panics(t, func() { NewEtcdPrefix("").Tail() }, "Empty EtcdPrefix")
	require.Equal(t, NewEtcdPrefix("aaa").Head(), NewEtcdPrefix(""))
	require.Equal(t, NewEtcdPrefix("aaa").Tail(), NewEtcdRelPrefix("aaa"))

	require.Panics(t, func() { NewEtcdKey("").Head() }, "Empty EtcdKey")
	require.Panics(t, func() { NewEtcdKey("").Tail() }, "Empty EtcdKey")
	require.Equal(t, NewEtcdKey("aaa").Head(), NewEtcdPrefix(""))
	require.Equal(t, NewEtcdKey("aaa").Tail(), NewEtcdRelKey("aaa"))
}

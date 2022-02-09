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

package dispatcher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTopicExpression(t *testing.T) {
	// valid topic expressions containg '{schema}'
	name, err := Parse("{schema}", "abc", "")
	require.Nil(t, err)
	require.Equal(t, name, "abc")

	name, err = Parse("abc{schema}", "def", "")
	require.Nil(t, err)
	require.Equal(t, name, "abcdef")

	name, err = Parse("{schema}def", "hello", "")
	require.Nil(t, err)
	require.Equal(t, name, "hellodef")

	name, err = Parse("abc{schema}def", "hello", "")
	require.Nil(t, err)
	require.Equal(t, name, "abchellodef")

	// always convert schema/table to lower case letters
	name, err = Parse("abc{schema}", "DEF", "")
	require.Nil(t, err)
	require.Equal(t, name, "abcdef")

	name, err = Parse("abc._-def{schema}abc", "HELLO", "")
	require.Nil(t, err)
	require.Equal(t, name, "abc._-defhelloabc")

	// always replace schema/table containing characters other than [A-Za-z0-9\._\-]
	// with the underscore '_'
	name, err = Parse("abc_def{schema}", "!@#$%^&*()_+.{}", "")
	require.Nil(t, err)
	require.Equal(t, name, "abc_def____________.__")

	name, err = Parse("abc{schema}def", "你好", "")
	require.Nil(t, err)
	require.Equal(t, name, "abc__def")

	// invalid topic expressions containing '{schema}'
	// the prefix must be [A-Za-z0-9\._\-]*
	_, err = Parse("{{{{schema}", "abc", "")
	require.NotNil(t, err)

	_, err = Parse("你好{schema}", "world", "")
	require.NotNil(t, err)

	// the suffix must be [A-Za-z0-9\._\-]*
	_, err = Parse("{schema}}}}}", "abc", "")
	require.NotNil(t, err)

	// invalid topic expressions not containing '{schema}'
	_, err = Parse("{xxx}", "abc", "")
	require.NotNil(t, err)

	_, err = Parse("abc{sch}", "def", "")
	require.NotNil(t, err)

	// valid topic expressions containing '{schema}_{table}'
	name, err = Parse("{schema}_{table}", "hello", "world")
	require.Nil(t, err)
	require.Equal(t, name, "hello_world")

	// always convert schema/table to lower case letters
	name, err = Parse("{schema}_{table}", "HELLO", "WORLD")
	require.Nil(t, err)
	require.Equal(t, name, "hello_world")

	name, err = Parse("{schema}_{table}", "hello", "!@#$%^&*")
	require.Nil(t, err)
	require.Equal(t, name, "hello_________")

	name, err = Parse("{schema}_{table}", "()_+.{}", "world")
	require.Nil(t, err)
	require.Equal(t, name, "____.___world")

	name, err = Parse("{schema}_{table}", "你好", "世界")
	require.Nil(t, err)
	require.Equal(t, name, "_____")

	// invalid topic expressions not containing '{schema}_{table}'
	_, err = Parse("{sch}_{tab}", "hello", "world")
	require.NotNil(t, err)
}

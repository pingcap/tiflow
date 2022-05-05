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

package sqlmodel

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestValuesHolder(t *testing.T) {
	t.Parallel()

	require.Equal(t, "()", valuesHolder(0))
	require.Equal(t, "(?)", valuesHolder(1))
	require.Equal(t, "(?,?)", valuesHolder(2))
}

func TestValidatorGenColData(t *testing.T) {
	res := ColValAsStr(1)
	require.Equal(t, "1", res)
	res = ColValAsStr(1.2)
	require.Equal(t, "1.2", res)
	res = ColValAsStr("abc")
	require.Equal(t, "abc", res)
	res = ColValAsStr([]byte{'\x01', '\x02', '\x03'})
	require.Equal(t, "\x01\x02\x03", res)
	res = ColValAsStr(decimal.NewFromInt(222123123))
	require.Equal(t, "222123123", res)
}

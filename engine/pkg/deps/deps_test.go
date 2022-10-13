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

package deps

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/dig"
)

type a struct {
	val int
}

type b struct {
	inner *a
}

type params struct {
	dig.In

	A *a
	B *b
}

func TestDepsBasics(t *testing.T) {
	t.Parallel()

	deps := NewDeps()
	err := deps.Provide(func() *a {
		return &a{val: 1}
	})
	require.NoError(t, err)

	out, err := deps.Construct(func(input *a) (*b, error) {
		return &b{inner: input}, nil
	})
	require.NoError(t, err)
	require.IsType(t, &b{}, out)
	require.Equal(t, &b{
		inner: &a{val: 1},
	}, out)

	err = deps.Provide(func(inner *a) *b {
		return &b{inner: inner}
	})
	require.NoError(t, err)

	var p params
	err = deps.Fill(&p)
	require.NoError(t, err)
	require.Equal(t, params{
		A: &a{val: 1},
		B: &b{inner: &a{val: 1}},
	}, p)
}

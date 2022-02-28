package deps

import (
	"testing"

	"go.uber.org/dig"

	"github.com/stretchr/testify/require"
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

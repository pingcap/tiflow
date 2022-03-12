package sqlmodel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValuesHolder(t *testing.T) {
	t.Parallel()

	require.Equal(t, "()", valuesHolder(0))
	require.Equal(t, "(?)", valuesHolder(1))
	require.Equal(t, "(?,?)", valuesHolder(2))
}

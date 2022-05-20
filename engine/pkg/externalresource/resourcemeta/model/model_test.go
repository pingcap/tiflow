package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseResourcePath(t *testing.T) {
	tp, suffix, err := ParseResourcePath("/local/my-local-resource/a/b/c")
	require.NoError(t, err)
	require.Equal(t, ResourceTypeLocalFile, tp)
	require.Equal(t, "my-local-resource/a/b/c", suffix)
}

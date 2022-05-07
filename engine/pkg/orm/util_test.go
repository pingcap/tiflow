package orm

import (
	"errors"
	"testing"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsNotFoundError(t *testing.T) {
	b := IsNotFoundError(cerrors.ErrMetaEntryNotFound.GenWithStackByArgs("error"))
	require.True(t, b)

	b = IsNotFoundError(cerrors.ErrMetaEntryNotFound.GenWithStack("err:%s", "error"))
	require.True(t, b)

	b = IsNotFoundError(cerrors.ErrMetaEntryNotFound.Wrap(errors.New("error")))
	require.True(t, b)

	b = IsNotFoundError(cerrors.ErrMetaNewClientFail.Wrap(errors.New("error")))
	require.False(t, b)

	b = IsNotFoundError(errors.New("error"))
	require.False(t, b)
}

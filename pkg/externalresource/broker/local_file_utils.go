package broker

import (
	"context"

	brStorage "github.com/pingcap/tidb/br/pkg/storage"

	derrors "github.com/hanfei1991/microcosm/pkg/errors"
)

func newBrStorageForLocalFile(filePath string) (brStorage.ExternalStorage, error) {
	backend, err := brStorage.ParseBackend(filePath, nil)
	if err != nil {
		return nil, err
	}
	ls, err := brStorage.New(context.Background(), backend, nil)
	if err != nil {
		return nil, derrors.ErrFailToCreateExternalStorage.Wrap(err)
	}
	return ls, nil
}

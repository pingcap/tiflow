package s3

import (
	"context"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
)

func newS3ExternalStorage(
	ctx context.Context, uri string, options *brStorage.S3BackendOptions,
) (brStorage.ExternalStorage, error) {
	opts := &brStorage.BackendOptions{
		S3: *options,
	}
	backEnd, err := brStorage.ParseBackend(uri, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Note that we may have network I/O here.
	ret, err := brStorage.New(ctx, backEnd, &brStorage.ExternalStorageOptions{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

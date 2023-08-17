package common

import (
	"github.com/pingcap/tiflow/pkg/compression"
	"github.com/pingcap/tiflow/pkg/errors"
)

func Compress(cc string, data []byte) ([]byte, error) {
	oldSize := len(data)
	compressed, err := compression.Encode(cc, data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newSize := len(compressed)
	_ = float64(oldSize) / float64(newSize) * 100

	return compressed, nil
}

package common

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/compression"
	"github.com/pingcap/tiflow/pkg/errors"
)

func Compress(changefeedID model.ChangeFeedID, cc string, data []byte) ([]byte, error) {
	oldSize := len(data)
	compressed, err := compression.Encode(cc, data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newSize := len(compressed)
	ratio := float64(oldSize) / float64(newSize) * 100

	compressionRatio.WithLabelValues(changefeedID.Namespace, changefeedID.ID).Observe(ratio)

	return compressed, nil
}

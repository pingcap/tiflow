package pdutil

import (
	"context"
	"strconv"

	cerror "github.com/pingcap/tiflow/pkg/errors"
	pd "github.com/tikv/pd/client"
)

const sourceIDName = "source_id"

// GetSourceID returns the source ID of the TiDB cluster that PD is belonged to.
func GetSourceID(ctx context.Context, pdClient pd.Client) (uint64, error) {
	// only nil in test case
	if pdClient == nil {
		return 1, nil
	}
	// The default value of sourceID is 1,
	// which means the sourceID is not changed by user.
	sourceID := uint64(1)
	sourceIDConfig, err := pdClient.LoadGlobalConfig(ctx, []string{sourceIDName})
	if err != nil {
		return 0, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if len(sourceIDConfig) != 0 && sourceIDConfig[0].Value != "" {
		sourceID, err = strconv.ParseUint(sourceIDConfig[0].Value, 10, 64)
		if err != nil {
			return 0, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
		}
	}
	return sourceID, nil
}

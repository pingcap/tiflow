package orm

import "strings"

// IsNotFoundError chesk whether the error is ErrMetaEntryNotFound
// TODO: refine me, need wrap error for api
func IsNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "ErrMetaEntryNotFound")
}

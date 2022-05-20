package orm

import "strings"

// IsNotFoundError checks whether the error is ErrMetaEntryNotFound
// TODO: refine me, need wrap error for api
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "ErrMetaEntryNotFound")
}

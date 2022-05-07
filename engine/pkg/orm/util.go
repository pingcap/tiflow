package orm

import "strings"

// TODO: refine me, need wrap error for api
func IsNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "ErrMetaEntryNotFound")
}

package dm

import "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"

// NewDMResourceID returns a ResourceID in DM's style. Currently only support local resource.
func NewDMResourceID(taskName, sourceName string) resourcemeta.ResourceID {
	return "/" + string(resourcemeta.ResourceTypeLocalFile) + "/" + taskName + "/" + sourceName
}

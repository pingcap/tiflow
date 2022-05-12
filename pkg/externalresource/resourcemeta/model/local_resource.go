package model

import (
	"path/filepath"

	libModel "github.com/hanfei1991/microcosm/lib/model"
)

// LocalFileResourceDescriptor contains necessary data
// to access a local file resource.
type LocalFileResourceDescriptor struct {
	BasePath     string
	Creator      libModel.WorkerID
	ResourceName ResourceName
}

// AbsolutePath returns the absolute path of the given resource
// in the local file system.
func (d *LocalFileResourceDescriptor) AbsolutePath() string {
	return filepath.Join(d.BasePath, d.Creator, d.ResourceName)
}

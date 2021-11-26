// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

const (
	gb                       = 1024 * 1024 * 1024
	dataDirAvailLowThreshold = 10 // percentage
)

// IsDirAndWritable checks a given path is directory and writable
func IsDirAndWritable(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return cerror.WrapError(cerror.ErrCheckDirWritable, err)
	}
	if !st.IsDir() {
		return cerror.WrapError(cerror.ErrCheckDirWritable, errors.Errorf("%s is not a directory", path))
	}
	return IsDirWritable(path)
}

// IsDirWritable checks if a dir is writable, return error nil means it is writable
func IsDirWritable(dir string) error {
	f := filepath.Join(dir, ".writable.test")
	if err := os.WriteFile(f, []byte(""), 0o600); err != nil {
		return cerror.WrapError(cerror.ErrCheckDirWritable, err)
	}
	return cerror.WrapError(cerror.ErrCheckDirWritable, os.Remove(f))
}

// IsDirReadWritable check if the dir is writable and readable by cdc server
func IsDirReadWritable(dir string) error {
	f := filepath.Join(dir, "file.test")
	if err := os.WriteFile(f, []byte(""), 0o600); err != nil {
		return cerror.WrapError(cerror.ErrCheckDirValid, err)
	}

	if _, err := os.ReadFile(f); err != nil {
		return cerror.WrapError(cerror.ErrCheckDirValid, err)
	}

	return cerror.WrapError(cerror.ErrCheckDirValid, os.Remove(f))
}

// DiskInfo present the disk amount information, in gb
type DiskInfo struct {
	All             uint64
	Used            uint64
	Free            uint64
	Avail           uint64
	AvailPercentage float32
}

func (d *DiskInfo) String() string {
	return fmt.Sprintf("{All: %+vGB; Used: %+vGB; Free: %+vGB; Available: %+vGB; Available Percentage: %+v%%}",
		d.All, d.Used, d.Free, d.Avail, d.AvailPercentage)
}

// CheckDataDirSatisfied check if the data-dir meet the requirement during server running
// the caller should guarantee that dir exist
func CheckDataDirSatisfied() error {
	conf := config.GetGlobalServerConfig()
	diskInfo, err := GetDiskInfo(conf.DataDir)
	if err != nil {
		return cerror.WrapError(cerror.ErrCheckDataDirSatisfied, err)
	}
	if diskInfo.AvailPercentage < dataDirAvailLowThreshold {
		failpoint.Inject("InjectCheckDataDirSatisfied", func() {
			log.Info("inject check data dir satisfied error")
			failpoint.Return(nil)
		})
		return cerror.WrapError(cerror.ErrCheckDataDirSatisfied, errors.Errorf("disk is almost full, TiCDC require that the disk mount data-dir "+
			"have 10%% available space, and the total amount has at least 500GB is preferred. disk info: %+v", diskInfo))
	}

	return nil
}

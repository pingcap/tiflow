// Copyright 2019 PingCAP, Inc.
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

package utils

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

// IsFileExists checks if file exists.
func IsFileExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	if f.IsDir() {
		return false
	}

	return true
}

// IsDirExists checks if dir exists.
func IsDirExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	if !f.IsDir() {
		return false
	}

	return true
}

// GetFileSize return the size of the file.
// NOTE: do not support to get the size of the directory now.
func GetFileSize(file string) (int64, error) {
	if !IsFileExists(file) {
		return 0, terror.ErrGetFileSize.Generate(file)
	}

	stat, err := os.Stat(file)
	if err != nil {
		return 0, terror.ErrGetFileSize.Delegate(err, file)
	}
	return stat.Size(), nil
}

// ParseFileSize parses the size in MiB from input.
func ParseFileSize(fileSizeStr string, defaultSize uint64) (uint64, error) {
	var fileSize uint64
	if len(fileSizeStr) == 0 {
		fileSize = defaultSize
	} else if fileSizeMB, err := strconv.ParseUint(fileSizeStr, 10, 64); err == nil {
		fileSize = fileSizeMB * units.MiB
	} else if size, err := units.RAMInBytes(fileSizeStr); err == nil {
		fileSize = uint64(size)
	} else {
		return 0, err
	}
	return fileSize, nil
}

// WriteFileAtomic writes file to temp and atomically move when everything else succeeds.
func WriteFileAtomic(filename string, data []byte, perm os.FileMode) error {
	dir, name := path.Dir(filename), path.Base(filename)
	f, err := os.CreateTemp(dir, name)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	f.Close()
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	} else {
		err = os.Chmod(f.Name(), perm)
	}
	if err != nil {
		err2 := os.Remove(f.Name())
		log.L().Warn("failed to remove the temporary file",
			zap.String("filename", f.Name()),
			zap.Error(err2))
		return err
	}
	return os.Rename(f.Name(), filename)
}

// CollectDirFiles gets files in path.
func CollectDirFiles(path string) (map[string]struct{}, error) {
	files := make(map[string]struct{})
	err := filepath.Walk(path, func(_ string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if f == nil {
			return nil
		}

		if f.IsDir() {
			return nil
		}

		name := strings.TrimSpace(f.Name())
		files[name] = struct{}{}
		return nil
	})

	return files, err
}

// GetDBFromDumpFilename extracts db name from dump filename.
func GetDBFromDumpFilename(filename string) (db string, ok bool) {
	if !strings.HasSuffix(filename, "-schema-create.sql") {
		return "", false
	}

	idx := strings.LastIndex(filename, "-schema-create.sql")
	return filename[:idx], true
}

// GetTableFromDumpFilename extracts db and table name from dump filename.
func GetTableFromDumpFilename(filename string) (db, table string, ok bool) {
	if !strings.HasSuffix(filename, "-schema.sql") {
		return "", "", false
	}

	idx := strings.LastIndex(filename, "-schema.sql")
	name := filename[:idx]
	fields := strings.Split(name, ".")
	if len(fields) != 2 {
		return "", "", false
	}
	return fields[0], fields[1], true
}

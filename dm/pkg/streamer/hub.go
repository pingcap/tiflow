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

package streamer

import (
	"path/filepath"
	"strings"
	"sync"

	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var (
	readerHub *ReaderHub // singleton instance
	once      sync.Once
)

// RelayLogInfo represents information for relay log.
type RelayLogInfo struct {
	TaskName     string
	SubDir       string
	SubDirSuffix int
	Filename     string
}

// Earlier checks whether this relay log file is earlier than the other.
func (info *RelayLogInfo) Earlier(other *RelayLogInfo) bool {
	if info.SubDirSuffix < other.SubDirSuffix {
		return true
	} else if info.SubDirSuffix > other.SubDirSuffix {
		return false
	}
	return strings.Compare(info.Filename, other.Filename) < 0
}

// String implements Stringer.String.
func (info *RelayLogInfo) String() string {
	return filepath.Join(info.SubDir, info.Filename)
}

// relayLogInfoHub holds information for all active relay logs.
type relayLogInfoHub struct {
	mu   sync.RWMutex
	logs map[string]RelayLogInfo
}

func newRelayLogInfoHub() *relayLogInfoHub {
	return &relayLogInfoHub{
		logs: map[string]RelayLogInfo{},
	}
}

func (h *relayLogInfoHub) update(taskName, subDir, filename string) error {
	_, suffix, err := utils.ParseRelaySubDir(subDir)
	if err != nil {
		return err
	}
	if !binlog.VerifyFilename(filename) {
		return terror.ErrBinlogInvalidFilename.Generatef("binlog filename %s not valid", filename)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs[taskName] = RelayLogInfo{
		TaskName:     taskName,
		SubDir:       subDir,
		SubDirSuffix: suffix,
		Filename:     filename,
	}
	return nil
}

func (h *relayLogInfoHub) remove(taskName string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.logs, taskName)
}

func (h *relayLogInfoHub) earliest() (taskName string, earliest *RelayLogInfo) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for name, info := range h.logs {
		var isEarlier bool
		if earliest == nil {
			isEarlier = true
		} else if info.Earlier(earliest) {
			isEarlier = true
		}
		if isEarlier {
			taskName = name
			clone := info
			earliest = &clone
		}
	}
	return
}

// ReaderHub holds information for all active Readers.
type ReaderHub struct {
	rlih *relayLogInfoHub
}

// GetReaderHub gets singleton instance of ReaderHub.
func GetReaderHub() *ReaderHub {
	once.Do(func() {
		readerHub = &ReaderHub{
			rlih: newRelayLogInfoHub(),
		}
	})
	return readerHub
}

// UpdateActiveRelayLog updates active relay log for taskName.
func (h *ReaderHub) UpdateActiveRelayLog(taskName, subDir, filename string) error {
	return h.rlih.update(taskName, subDir, filename)
}

// RemoveActiveRelayLog removes active relay log for taskName.
func (h *ReaderHub) RemoveActiveRelayLog(taskName string) {
	h.rlih.remove(taskName)
}

// EarliestActiveRelayLog implements RelayOperator.EarliestActiveRelayLog.
func (h *ReaderHub) EarliestActiveRelayLog() *RelayLogInfo {
	_, rli := h.rlih.earliest()
	return rli
}

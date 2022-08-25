// Copyright 2022 PingCAP, Inc.
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

package binlogstream

import (
	"time"

	"github.com/pingcap/tiflow/dm/relay"
)

// BinlogType represents binlog type from syncer's view.
type BinlogType uint8

const (
	// RemoteBinlog means syncer is connected to MySQL and reading binlog.
	RemoteBinlog BinlogType = iota + 1
	// LocalBinlog means syncer is reading binlog from relay log.
	LocalBinlog
)

// RelayToBinlogType converts relay.Process to BinlogType.
func RelayToBinlogType(relay relay.Process) BinlogType {
	if relay != nil {
		return LocalBinlog
	}

	return RemoteBinlog
}

func (t BinlogType) String() string {
	switch t {
	case RemoteBinlog:
		return "remote"
	case LocalBinlog:
		return "local"
	default:
		return "unknown"
	}
}

// the minimal interval to retry reset binlog streamer.
var minErrorRetryInterval = 1 * time.Minute

type retryStrategy interface {
	CanRetry(error) bool
}

type alwaysRetryStrategy struct{}

func (s alwaysRetryStrategy) CanRetry(error) bool {
	return true
}

// maxIntervalRetryStrategy allows retry when the retry interval is greater than the set interval.
type maxIntervalRetryStrategy struct {
	interval    time.Duration
	lastErr     error
	lastErrTime time.Time
}

func (s *maxIntervalRetryStrategy) CanRetry(err error) bool {
	if err == nil {
		return true
	}

	now := time.Now()
	lastErrTime := s.lastErrTime
	s.lastErrTime = now
	s.lastErr = err
	return lastErrTime.Add(s.interval).Before(now)
}

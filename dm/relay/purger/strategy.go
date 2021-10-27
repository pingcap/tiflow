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

package purger

import "github.com/pingcap/dm/pkg/streamer"

type strategyType uint32

const (
	strategyNone strategyType = iota
	strategyInactive
	strategyFilename
	strategyTime
	strategySpace
)

func (s strategyType) String() string {
	switch s {
	case strategyInactive:
		return "inactive strategy"
	case strategyFilename:
		return "filename strategy"
	case strategyTime:
		return "time strategy"
	case strategySpace:
		return "space strategy"
	default:
		return "unknown strategy"
	}
}

// PurgeStrategy represents a relay log purge strategy
// two purge behaviors
//   1. purge in the background
//   2. do one time purge process
// a strategy can support both or one of them.
type PurgeStrategy interface {
	// Check checks whether need to do the purge in the background automatically
	Check(args interface{}) (bool, error)

	// Do does the purge process one time
	Do(args interface{}) error

	// Purging indicates whether is doing purge
	Purging() bool

	// Type returns the strategy type
	Type() strategyType
}

// StrategyArgs represents args needed by purge strategy.
type StrategyArgs interface {
	// SetActiveRelayLog sets active relay log info in args
	// this should be called before do the purging
	SetActiveRelayLog(active *streamer.RelayLogInfo)
}

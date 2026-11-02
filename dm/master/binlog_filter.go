// Copyright 2026 PingCAP, Inc.
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

package master

import (
	"fmt"
	"strings"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// checkDuplicateBinlogFilterRules checks newly submitted task configurations only.
// Do not call it from SubTaskConfig.Adjust or NewBinlogEvent: existing tasks must
// remain loadable after an upgrade, including tasks with legacy invalid events.
func checkDuplicateBinlogFilterRules(cfg *config.SubTaskConfig) error {
	seen := make(map[[2]string]int, len(cfg.FilterRules))
	for i, rule := range cfg.FilterRules {
		if rule == nil {
			continue
		}
		pattern := [2]string{rule.SchemaPattern, rule.TablePattern}
		if !cfg.CaseSensitive {
			pattern[0] = strings.ToLower(pattern[0])
			pattern[1] = strings.ToLower(pattern[1])
		}
		if first, ok := seen[pattern]; ok {
			return terror.ErrConfigBinlogEventFilter.Delegate(fmt.Errorf(
				"source %q: binlog filter rules %d and %d have duplicate schema-pattern %q and table-pattern %q; use a single rule for each schema/table pattern",
				cfg.SourceID, first+1, i+1, rule.SchemaPattern, rule.TablePattern))
		}
		seen[pattern] = i
	}
	return nil
}

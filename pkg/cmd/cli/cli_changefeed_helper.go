// Copyright 2021 PingCAP, Inc.
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

package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	// tsGapWarning specifies the OOM threshold.
	// 1 day in milliseconds
	tsGapWarning = 86400 * 1000
)

// confirmLargeDataGap checks if a large data gap is used.
func confirmLargeDataGap(cmd *cobra.Command, currentPhysical int64, startTs uint64) error {
	tsGap := currentPhysical - oracle.ExtractPhysical(startTs)

	if tsGap > tsGapWarning {
		cmd.Printf("Replicate lag (%s) is larger than 1 days, "+
			"large data may cause OOM, confirm to continue at your own risk [Y/N]\n",
			time.Duration(tsGap)*time.Millisecond,
		)
		var yOrN string
		_, err := fmt.Scan(&yOrN)
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
			return errors.NewNoStackError("abort changefeed create or resume")
		}
	}

	return nil
}

// confirmIgnoreIneligibleTables confirm if user need to ignore ineligible tables.
// If ignore it will return true.
func confirmIgnoreIneligibleTables(cmd *cobra.Command) (bool, error) {
	cmd.Printf("Could you agree to ignore those tables, and continue to replicate [Y/N]\n")
	var yOrN string
	_, err := fmt.Scan(&yOrN)
	if err != nil {
		return false, err
	}
	if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
		cmd.Printf("No changefeed is created because you don't want to ignore some tables.\n")
		return false, errors.NewNoStackError("abort changefeed create or resume")
	}

	return true, nil
}

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
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	// tsGapWarning specifies the OOM threshold.
	// 1 day in milliseconds
	tsGapWarning = 86400 * 1000
)

func readInput() (string, error) {
	reader := bufio.NewReader(os.Stdin)
	msg, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(msg), nil
}

func readYOrN(cmd *cobra.Command) bool {
	var yOrN string
	_, err := fmt.Scan(&yOrN)
	if err != nil {
		cmd.Printf("Received invalid input: %s, abort the command.\n", err.Error())
		return false
	}
	if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
		return false
	}
	return true
}

// confirmLargeDataGap checks if a large data gap is used.
func confirmLargeDataGap(cmd *cobra.Command, currentPhysical int64, startTs uint64, command string) error {
	tsGap := currentPhysical - oracle.ExtractPhysical(startTs)

	if tsGap > tsGapWarning {
		cmd.Printf("Replicate lag (%s) is larger than 1 days, "+
			"large data may cause OOM, confirm to continue at your own risk [Y/N]\n",
			time.Duration(tsGap)*time.Millisecond,
		)
		confirmed := readYOrN(cmd)
		if !confirmed {
			cmd.Printf("Abort changefeed %s.\n", command)
			return cerror.ErrCliAborted.FastGenByArgs(fmt.Sprintf("cli changefeed %s", command))
		}
	}

	return nil
}

// confirmOverwriteCheckpointTs prompts risk warnings when users are trying to
// overwrite the checkpointTs
func confirmOverwriteCheckpointTs(
	cmd *cobra.Command, changefeedID string, checkpointTs uint64,
) error {
	cmd.Printf("You are overwriting the checkpoint of changefeed(%s) to %d,"+
		" which may lead to data loss or data duplication.\nConfirm that you know"+
		" what this command will do and use it at your own risk [Y/N]", changefeedID, checkpointTs)
	confirmed := readYOrN(cmd)
	if !confirmed {
		cmd.Printf("Abort changefeed resume.\n")
		return cerror.ErrCliAborted.FastGenByArgs("cli changefeed resume")
	}

	return nil
}

// confirmIgnoreIneligibleTables confirm if user need to ignore ineligible tables.
// If ignore it will return true.
func confirmIgnoreIneligibleTables(cmd *cobra.Command) (bool, error) {
	cmd.Printf("Could you agree to ignore those tables, and continue to replicate [Y/N]\n" +
		"Note: If you don't want to ignore those tables, please set `force-replicate to` true " +
		"in the changefeed config file.\n")
	confirmed := readYOrN(cmd)
	if !confirmed {
		cmd.Printf("No changefeed is created because you don't want to ignore some tables.\n")
		return false, cerror.ErrCliAborted.FastGenByArgs("cli changefeed create")
	}

	return true, nil
}

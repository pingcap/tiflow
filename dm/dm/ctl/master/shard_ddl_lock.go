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

package master

import (
	"github.com/spf13/cobra"
)

// NewShardDDLLockCmd creates a ShardDDLLock command.
func NewShardDDLLockCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shard-ddl-lock [task]",
		Short: "maintain or show shard-ddl locks information",
		RunE:  showDDLLocksFunc,
	}
	cmd.AddCommand(
		newDDLLockUnlockCmd(),
	)

	return cmd
}

func newDDLLockUnlockCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unlock <lock-id>",
		Short: "Unlock un-resolved DDL locks forcely",
		RunE:  unlockDDLLockFunc,
	}
	cmd.Flags().StringP("owner", "o", "", "source to replace the default owner")
	cmd.Flags().BoolP("force-remove", "f", false, "force to remove DDL lock")
	return cmd
}

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
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/spf13/cobra"
)

// NewSourceTableSchemaCmd creates a SourceTableSchema command.
func NewSourceTableSchemaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binlog-schema <task-name> <table-filter1> <table-filter2> ...",
		Short: "manage or show source-table schema schemas",
		RunE:  sourceTableSchemaList,
	}
	cmd.AddCommand(
		newSourceTableSchemaUpdateCmd(),
		newSourceTableSchemaDeleteCmd(),
	)

	return cmd
}

func sourceTableSchemaList(cmd *cobra.Command, args []string) error {
	if len(args) < 3 {
		return cmd.Help()
	}
	taskName := common.GetTaskNameFromArgOrFile(args[0])
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}
	database := args[1]
	table := args[2]
	return sendOperateSchemaRequest(pb.SchemaOp_GetSchema, taskName, sources, database, table, "", false, false)
}

func newSourceTableSchemaUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <task-name> <table-filter1> <table-filter2> ... <schema-file>",
		Short: "update tables schema structures",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 3 {
				return cmd.Help()
			}
			taskName := common.GetTaskNameFromArgOrFile(args[0])
			sources, err := common.GetSourceArgs(cmd)
			if err != nil {
				return err
			}
			database := args[1]
			table := args[2]
			schemaFile := args[3]
			schemaContent, err := common.GetFileContent(schemaFile)
			if err != nil {
				return err
			}
			flush, err := cmd.Flags().GetBool("flush")
			if err != nil {
				return err
			}
			sync, err := cmd.Flags().GetBool("sync")
			if err != nil {
				return err
			}
			return sendOperateSchemaRequest(pb.SchemaOp_SetSchema, taskName, sources, database, table, string(schemaContent), flush, sync)
		},
	}
	cmd.Flags().Bool("flush", true, "flush the table info and checkpoint immediately")
	cmd.Flags().Bool("sync", false, "sync the table info to master to resolve shard ddl lock, only for optimistic mode now")
	cmd.Flags().Bool("from-source", false, "use the schema from upstream database as the schema of the specified tables")
	cmd.Flags().Bool("from-target", false, "use the schema from downstream database as the schema of the specified tables")
	return cmd
}

func newSourceTableSchemaDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <task-name> <table-filter1> <table-filter2> ...",
		Short: "delete tables schema structures",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 3 {
				return cmd.Help()
			}
			taskName := common.GetTaskNameFromArgOrFile(args[0])
			sources, err := common.GetSourceArgs(cmd)
			if err != nil {
				return err
			}
			database := args[1]
			table := args[2]
			return sendOperateSchemaRequest(pb.SchemaOp_RemoveSchema, taskName, sources, database, table, "", false, false)
		},
	}
	return cmd
}

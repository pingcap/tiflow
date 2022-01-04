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
	"errors"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"

	"github.com/spf13/cobra"
)

// NewSourceTableSchemaCmd creates a SourceTableSchema command.
func NewSourceTableSchemaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binlog-schema <task-name> <database> <table>",
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
	request := &pb.OperateSchemaRequest{
		Op:         pb.SchemaOp_GetSchema,
		Task:       taskName,
		Sources:    sources,
		Database:   database,
		Table:      table,
		Schema:     "",
		Flush:      false,
		Sync:       false,
		FromSource: false,
		FromTarget: false,
	}
	return sendOperateSchemaRequest(request)
}

func newSourceTableSchemaUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <task-name> <database> <table> [schema-file]",
		Short: "update tables schema structures",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 3 {
				return cmd.Help()
			}

			var (
				taskName, database, table           string
				sources                             []string
				schemaContent                       []byte
				flush, sync, fromSource, fromTarget bool
				err                                 error
			)

			fromSource, err = cmd.Flags().GetBool("from-source")
			if err != nil {
				return err
			}
			fromTarget, err = cmd.Flags().GetBool("from-target")
			if err != nil {
				return err
			}

			if fromSource && fromTarget {
				common.PrintLinesf("from-source and from-target can not be used together")
				return errors.New("please check output to see error")
			}

			if !fromSource && !fromTarget && len(args) < 4 {
				return cmd.Help()
			}

			if len(args) == 4 && (fromSource || fromTarget) {
				common.PrintLinesf("can not set schema-file when use from-source or from-target")
				return errors.New("please check output to see error")
			}

			taskName = common.GetTaskNameFromArgOrFile(args[0])
			sources, err = common.GetSourceArgs(cmd)
			if err != nil {
				return err
			}
			database = args[1]
			table = args[2]

			if !fromSource && !fromTarget {
				schemaFile := args[3]
				schemaContent, err = common.GetFileContent(schemaFile)
				if err != nil {
					return err
				}
			}

			flush, err = cmd.Flags().GetBool("flush")
			if err != nil {
				return err
			}
			sync, err = cmd.Flags().GetBool("sync")
			if err != nil {
				return err
			}
			request := &pb.OperateSchemaRequest{
				Op:         pb.SchemaOp_SetSchema,
				Task:       taskName,
				Sources:    sources,
				Database:   database,
				Table:      table,
				Schema:     string(schemaContent),
				Flush:      flush,
				Sync:       sync,
				FromSource: fromSource,
				FromTarget: fromTarget,
			}
			return sendOperateSchemaRequest(request)
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
		Use:   "delete <task-name> <database> <table>",
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
			request := &pb.OperateSchemaRequest{
				Op:         pb.SchemaOp_SetSchema,
				Task:       taskName,
				Sources:    sources,
				Database:   database,
				Table:      table,
				Schema:     "",
				Flush:      false,
				Sync:       false,
				FromSource: false,
				FromTarget: false,
			}
			return sendOperateSchemaRequest(request)
		},
	}
	return cmd
}

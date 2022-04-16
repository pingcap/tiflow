// Copyright 2020 PingCAP, Inc.
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
	"context"
	"errors"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

// NewOperateSchemaCmd creates a OperateSchema command.
func NewOperateSchemaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "operate-schema <operate-type> <-s source ...> <task-name | task-file> <-d database> <-t table> [schema-file] [--flush] [--sync]",
		Short:  "`get`/`set`/`remove` the schema for an upstream table",
		Hidden: true,
		RunE:   operateSchemaCmd,
	}
	cmd.Flags().StringP("database", "d", "", "database name of the table")
	cmd.Flags().StringP("table", "t", "", "table name")
	cmd.Flags().Bool("flush", true, "flush the table info and checkpoint immediately")
	cmd.Flags().Bool("sync", true, "sync the table info to master to resolve shard ddl lock, only for optimistic mode now")
	return cmd
}

func convertSchemaOpType(t string) pb.SchemaOp {
	switch t {
	case "get":
		return pb.SchemaOp_GetSchema
	case "set":
		return pb.SchemaOp_SetSchema
	case "remove":
		return pb.SchemaOp_RemoveSchema
	default:
		return pb.SchemaOp_InvalidSchemaOp
	}
}

// operateSchemaCmd does the operate schema request.
func operateSchemaCmd(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) < 2 || len(cmd.Flags().Args()) > 3 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	opType := cmd.Flags().Arg(0)
	taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(1))
	schemaFile := cmd.Flags().Arg(2)
	var err error
	var schemaContent []byte
	op := convertSchemaOpType(opType)
	switch op {
	case pb.SchemaOp_InvalidSchemaOp:
		common.PrintLinesf("invalid operate '%s' on schema", opType)
		return errors.New("please check output to see error")
	case pb.SchemaOp_SetSchema:
		if schemaFile == "" {
			common.PrintLinesf("must sepcify schema file for 'set' operation")
			return errors.New("please check output to see error")
		}
		schemaContent, err = common.GetFileContent(schemaFile)
		if err != nil {
			return err
		}
	default:
		if schemaFile != "" {
			common.PrintLinesf("schema file will be ignored for 'get'/'delete' operation")
		}
	}

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	} else if len(sources) == 0 {
		common.PrintLinesf("must specify at least one source (`-s` / `--source`)")
		return errors.New("please check output to see error")
	}
	database, err := cmd.Flags().GetString("database")
	if err != nil {
		return err
	} else if database == "" {
		common.PrintLinesf("must specify 'database'")
		return errors.New("please check output to see error")
	}
	table, err := cmd.Flags().GetString("table")
	if err != nil {
		return err
	} else if table == "" {
		common.PrintLinesf("must specify 'table'")
		return errors.New("please check output to see error")
	}

	flush, err := cmd.Flags().GetBool("flush")
	if err != nil {
		return err
	}
	sync, err := cmd.Flags().GetBool("sync")
	if err != nil {
		return err
	}
	request := &pb.OperateSchemaRequest{
		Op:         op,
		Task:       taskName,
		Sources:    sources,
		Database:   database,
		Table:      table,
		Schema:     string(schemaContent),
		Flush:      flush,
		Sync:       sync,
		FromSource: false,
		FromTarget: false,
	}
	return sendOperateSchemaRequest(request)
}

func sendOperateSchemaRequest(request *pb.OperateSchemaRequest) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.OperateSchemaResponse{}
	err := common.SendRequest(
		ctx,
		"OperateSchema",
		request,
		&resp,
	)
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}

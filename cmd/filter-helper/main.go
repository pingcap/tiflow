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

package main

import (
	"fmt"
	"strings"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/spf13/cobra"
)

var (
	table   string
	ddl     string
	cfgPath string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "TiCDC filter helper, use to check whether your filter config works as expected",
		Short: "A tool to check table and ddl query against filter rules",
		Run:   runFilter,
	}
	rootCmd.Flags().StringVarP(&cfgPath, "config", "c", "", "changefeed config file path")
	rootCmd.Flags().StringVarP(&table, "table", "t", "", "table name, format: [schema].[table] ")
	rootCmd.Flags().StringVarP(&ddl, "ddl", "d", "", "ddl query")
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
	}
}

func runFilter(cmd *cobra.Command, args []string) {
	// fmt.Printf("Filter Rules: %v\n", filterRules)
	// fmt.Printf("Schema Name: %s\n", schemaName)
	// fmt.Printf("Table Name: %s\n", tableName)
	cfg := &config.ReplicaConfig{}
	err := util.StrictDecodeFile(cfgPath, "cdc filter helper", cfg)
	if err != nil {
		fmt.Printf("decode config file error: %v\n", err)
		return
	}
	ft, err := filter.NewFilter(cfg, "")
	if err != nil {
		fmt.Printf("filter create error: %v\n", err)
		return
	}
	tableAndSchema := strings.Split(table, ".")
	if len(tableAndSchema) != 2 {
		fmt.Printf("the input format is invalid, only support {schema}.{table}: %s\n", table)
		return
	}

	target := "table"
	if ddl != "" {
		target = "ddl"
	}

	switch target {
	case "table":
		matched := !ft.ShouldIgnoreTable(tableAndSchema[0], tableAndSchema[1])
		if matched {
			fmt.Printf("Table: %s, Matched filter rule\n", table)
			return
		}
		fmt.Printf("Table: %s, Not matched filter rule\n", table)
	case "ddl":
		ddlType := timodel.ActionCreateTable
		discard := ft.ShouldDiscardDDL(ddlType, tableAndSchema[0], tableAndSchema[1])
		if discard {
			fmt.Printf("DDL: %s, should be discard by event filter rule\n", ddl)
			return
		}
		ignored, err := ft.ShouldIgnoreDDLEvent(&model.DDLEvent{
			StartTs: uint64(0),
			Query:   ddl,
			Type:    ddlType,
			TableInfo: &model.TableInfo{
				TableName: model.TableName{
					Schema: tableAndSchema[0],
					Table:  tableAndSchema[1],
				},
			},
		})
		if err != nil {
			fmt.Printf("filter ddl error: %s, error: %v\n", ddl, err)
			return
		}
		if ignored {
			fmt.Printf("DDL: %s, should be ignored by event filter rule\n", ddl)
			return
		}
		fmt.Printf("DDL: %s, should not be discard by event filter rule\n", ddl)
	default:
		fmt.Printf("unknown target: %s", target)

	}
}

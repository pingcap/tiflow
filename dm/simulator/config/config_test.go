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

// Package config is the configuration definitions used by the simulator.
package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	tableConfig := &TableConfig{
		DatabaseName: "games",
		TableName:    "members",
		Columns: []*ColumnDefinition{
			{
				ColumnName: "id",
				DataType:   "int",
				DataLen:    11,
			},
			{
				ColumnName: "name",
				DataType:   "varchar",
				DataLen:    255,
			},
			{
				ColumnName: "age",
				DataType:   "int",
				DataLen:    11,
			},
			{
				ColumnName: "team_id",
				DataType:   "int",
				DataLen:    11,
			},
		},
		UniqueKeyColumnNames: []string{"id", "name"},
	}
	require.Equal(t, "CREATE TABLE `games`.`members`(`id` int(11),`name` varchar(255),`age` int(11),`team_id` int(11),UNIQUE KEY `id_name`(`id`,`name`))", tableConfig.GenCreateTable())
}

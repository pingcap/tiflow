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

// TableConfig is the sub config for describing a simulating table in the data source.
type TableConfig struct {
	TableID              string              `yaml:"id"`
	DatabaseName         string              `yaml:"db"`
	TableName            string              `yaml:"table"`
	Columns              []*ColumnDefinition `yaml:"columns"`
	UniqueKeyColumnNames []string            `yaml:"unique_keys"`
}

// ColumnDefinition is the sub config for describing a column in a simulating table.
type ColumnDefinition struct {
	ColumnName string `yaml:"name"`
	DataType   string `yaml:"type"`
	DataLen    int    `yaml:"length"`
}

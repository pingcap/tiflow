// Copyright 2025 PingCAP, Inc.
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

package rules

import "github.com/pingcap/tidb/pkg/parser/ast"

// MariaDBSpecificRule removes MariaDB-only table options.
type MariaDBSpecificRule struct{}

func (r *MariaDBSpecificRule) Name() string { return "MariaDBSpecific" }

func (r *MariaDBSpecificRule) Description() string {
	return "Remove MariaDB-only table options and syntax"
}

func (r *MariaDBSpecificRule) Priority() int { return 90 }

func (r *MariaDBSpecificRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		return hasMariaDBSpecificOptions(n.Options)
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if hasMariaDBSpecificOptions(spec.Options) {
				return true
			}
		}
	}
	return false
}

func (r *MariaDBSpecificRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		n.Options = filterMariaDBSpecificOptions(n.Options)
		return n, nil
	case *ast.AlterTableStmt:
		specs := n.Specs[:0]
		for _, spec := range n.Specs {
			spec.Options = filterMariaDBSpecificOptions(spec.Options)
			if len(spec.Options) == 0 && isOptionOnlyAlterSpec(spec) {
				continue
			}
			specs = append(specs, spec)
		}
		n.Specs = specs
		return n, nil
	}
	return node, nil
}

func hasMariaDBSpecificOptions(options []*ast.TableOption) bool {
	for _, opt := range options {
		if mariaDBSpecificTableOptions[opt.Tp] {
			return true
		}
	}
	return false
}

func filterMariaDBSpecificOptions(options []*ast.TableOption) []*ast.TableOption {
	if len(options) == 0 {
		return options
	}
	filtered := options[:0]
	for _, opt := range options {
		if mariaDBSpecificTableOptions[opt.Tp] {
			continue
		}
		filtered = append(filtered, opt)
	}
	return filtered
}

func isOptionOnlyAlterSpec(spec *ast.AlterTableSpec) bool {
	if spec == nil {
		return false
	}
	switch spec.Tp {
	case ast.AlterTableOption, ast.AlterTableAttributes, ast.AlterTablePartitionAttributes, ast.AlterTablePartitionOptions:
		return true
	default:
		return false
	}
}

var mariaDBSpecificTableOptions = map[ast.TableOptionType]bool{
	ast.TableOptionAvgRowLength:     true,
	ast.TableOptionCheckSum:         true,
	ast.TableOptionCompression:      true,
	ast.TableOptionConnection:       true,
	ast.TableOptionPassword:         true,
	ast.TableOptionKeyBlockSize:     true,
	ast.TableOptionMaxRows:          true,
	ast.TableOptionMinRows:          true,
	ast.TableOptionDelayKeyWrite:    true,
	ast.TableOptionRowFormat:        true,
	ast.TableOptionStatsPersistent:  true,
	ast.TableOptionStatsAutoRecalc:  true,
	ast.TableOptionPackKeys:         true,
	ast.TableOptionTablespace:       true,
	ast.TableOptionNodegroup:        true,
	ast.TableOptionDataDirectory:    true,
	ast.TableOptionIndexDirectory:   true,
	ast.TableOptionStorageMedia:     true,
	ast.TableOptionStatsSamplePages: true,
	ast.TableOptionInsertMethod:     true,
	ast.TableOptionTableCheckSum:    true,
	ast.TableOptionUnion:            true,
	ast.TableOptionEncryption:       true,
	ast.TableOptionStatsBuckets:     true,
	ast.TableOptionStatsTopN:        true,
	ast.TableOptionStatsColsChoice:  true,
	ast.TableOptionStatsColList:     true,
	ast.TableOptionStatsSampleRate:  true,
}

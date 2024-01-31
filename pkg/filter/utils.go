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

package filter

import (
	"fmt"

	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	tifilter "github.com/pingcap/tidb/pkg/util/filter"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// isSysSchema returns true if the given schema is a system schema
func isSysSchema(db string) bool {
	return tifilter.IsSystemSchema(db)
}

// VerifyTableRules checks the table filter rules in the configuration
// and returns an invalid rule error if the verification fails,
// otherwise it will return a table filter.
func VerifyTableRules(cfg *config.FilterConfig) (tfilter.Filter, error) {
	var f tfilter.Filter
	var err error
	if len(cfg.Rules) != 0 {
		rules := cfg.Rules
		if len(rules) == 0 {
			rules = []string{"*.*"}
		}
		f, err = tfilter.Parse(rules)
	}
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, cfg)
	}

	return f, nil
}

// ddlToEventType get event type from ddl query.
func ddlToEventType(jobType timodel.ActionType) bf.EventType {
	evenType, ok := ddlWhiteListMap[jobType]
	if ok {
		return evenType
	} else {
		return bf.NullEvent
	}
}

// isAlterTable returns true if the given job type is alter table's subtype.
func isAlterTable(jobType timodel.ActionType) bool {
	alterTableSubType := []timodel.ActionType{
		timodel.ActionAddColumn,
		timodel.ActionDropColumn,
		timodel.ActionModifyColumn,
		timodel.ActionSetDefaultValue,
	}

	for _, t := range alterTableSubType {
		if t == jobType {
			return true
		}
	}

	return false
}

// SupportedEventTypes returns the supported event types.
func SupportedEventTypes() []bf.EventType {
	var supportedEventTypes = []bf.EventType{
		bf.AllDML,
		bf.AllDDL,

		// dml events
		bf.InsertEvent,
		bf.UpdateEvent,
		bf.DeleteEvent,

		// ddl events
		bf.AlterTable,
		bf.CreateSchema,
		bf.DropSchema,
	}

	for _, ddlType := range ddlWhiteListMap {
		supportedEventTypes = append(supportedEventTypes, ddlType)
	}
	return supportedEventTypes
}

func completeExpression(suffix string) string {
	if suffix == "" {
		return suffix
	}
	return fmt.Sprintf("select * from t where %s", suffix)
}

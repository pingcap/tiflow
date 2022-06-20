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

package checker

import (
	"context"
	"database/sql"

	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/filter"

	onlineddl "github.com/pingcap/tiflow/dm/syncer/online-ddl-tools"
)

type OnlineDDLChecker struct {
	db           *sql.DB
	onlineDDL    onlineddl.OnlinePlugin
	bwlist       *filter.Filter
	checkSchemas map[string]struct{}
}

func NewOnlineDDLChecker(db *sql.DB, checkSchemas map[string]struct{}, onlineDDL onlineddl.OnlinePlugin, bwlist *filter.Filter) RealChecker {
	return &OnlineDDLChecker{
		db:           db,
		checkSchemas: checkSchemas,
		onlineDDL:    onlineDDL,
		bwlist:       bwlist,
	}
}

func (c *OnlineDDLChecker) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check if in online ddl phase",
		State: StateSuccess,
		Extra: "online ddl",
	}

	for schema := range c.checkSchemas {
		tableList, err := dbutil.GetTables(ctx, c.db, schema)
		if err != nil {
			markCheckError(r, err)
			return r
		}
		realTables := []*filter.Table{}
		for _, table := range tableList {
			if c.onlineDDL.TableType(table) == onlineddl.GhostTable {
				realTable := c.onlineDDL.RealName(table)
				realTables = append(realTables, &filter.Table{
					Schema: schema,
					Name:   realTable,
				})
			}
		}
		tables := c.bwlist.Apply(realTables)
		if len(tables) != 0 {
			r.State = StateFailure
			r.Extra = "please wait the online-ddl over"
			r.Errors = append(r.Errors, NewError("your ddl is in pt/ghost online-ddl"))
			return r
		}
	}

	return r
}

func (c *OnlineDDLChecker) Name() string {
	return "online ddl checker"
}

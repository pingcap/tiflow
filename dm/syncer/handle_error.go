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

package syncer

import (
	"context"
	"fmt"

	"github.com/pingcap/parser"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/parser/ast"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
)

// HandleError handle error for syncer.
func (s *Syncer) HandleError(ctx context.Context, req *pb.HandleWorkerErrorRequest) error {
	pos := req.BinlogPos

	if len(pos) == 0 {
		startLocation, isQueryEvent := s.getErrLocation()
		if startLocation == nil {
			return fmt.Errorf("source '%s' has no error", s.cfg.SourceID)
		}
		if !isQueryEvent {
			return fmt.Errorf("only support to handle ddl error currently, see https://docs.pingcap.com/tidb-data-migration/stable/error-handling for other errors")
		}
		pos = startLocation.Position.String()
	} else {
		startLocation, err := binlog.VerifyBinlogPos(pos)
		if err != nil {
			return err
		}
		pos = startLocation.String()
	}

	events := make([]*replication.BinlogEvent, 0)
	var err error
	if req.Op == pb.ErrorOp_Replace {
		events, err = s.genEvents(ctx, req.Sqls)
		if err != nil {
			return err
		}
	}

	// remove outdated operators when add operator
	err = s.errOperatorHolder.RemoveOutdated(s.checkpoint.FlushedGlobalPoint())
	if err != nil {
		return err
	}

	err = s.errOperatorHolder.Set(pos, req.Op, events)
	if err != nil {
		return err
	}

	return nil
}

func (s *Syncer) genEvents(ctx context.Context, sqls []string) ([]*replication.BinlogEvent, error) {
	events := make([]*replication.BinlogEvent, 0)

	parser2, err := s.fromDB.GetParser(ctx)
	if err != nil {
		s.tctx.L().Error("failed to get SQL mode specified parser from upstream, using default SQL mode instead")
		parser2 = parser.New()
	}

	for _, sql := range sqls {
		node, err := parser2.ParseOneStmt(sql, "", "")
		if err != nil {
			return nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "sql %s", sql)
		}

		switch node.(type) {
		case ast.DDLNode:
			tables, err := parserpkg.FetchDDLTables("", node, s.SourceTableNamesFlavor)
			if err != nil {
				return nil, err
			}

			schema := tables[0].Schema
			if len(schema) == 0 {
				return nil, terror.ErrSyncerUnitInjectDDLWithoutSchema.Generate(sql)
			}
			events = append(events, genQueryEvent([]byte(schema), []byte(sql)))
		default:
			// TODO: support DML
			return nil, terror.ErrSyncerReplaceEvent.New("only support replace with DDL currently")
		}
	}
	return events, nil
}

// genQueryEvent generate QueryEvent with empty EventSize and LogPos.
func genQueryEvent(schema, query []byte) *replication.BinlogEvent {
	header := &replication.EventHeader{
		EventType: replication.QUERY_EVENT,
	}
	queryEvent := &replication.QueryEvent{
		Schema: schema,
		Query:  query,
	}
	e := &replication.BinlogEvent{
		Header: header,
		Event:  queryEvent,
	}
	return e
}

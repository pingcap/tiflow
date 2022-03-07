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

package event

import (
	"bytes"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// GenDDLEvents generates binlog events for DDL statements.
// events: [GTIDEvent, QueryEvent]
func GenDDLEvents(flavor string, serverID, latestPos uint32, latestGTID gtid.Set, schema, query string, genGTID, anonymousGTID bool, ts int64) (*DDLDMLResult, error) {
	if ts == 0 {
		ts = time.Now().Unix()
	}
	// GTIDEvent, increase GTID first
	latestGTID, err := GTIDIncrease(flavor, latestGTID)
	if err != nil {
		return nil, terror.Annotatef(err, "increase GTID %s", latestGTID)
	}
	var gtidEv *replication.BinlogEvent
	if genGTID {
		gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, latestGTID, anonymousGTID, ts)
		if err != nil {
			return nil, terror.Annotate(err, "generate GTIDEvent")
		}
		latestPos = gtidEv.Header.LogPos
	}

	// QueryEvent
	header := &replication.EventHeader{
		Timestamp: uint32(ts),
		ServerID:  serverID,
		Flags:     defaultHeaderFlags,
	}
	queryEv, err := GenQueryEvent(header, latestPos, defaultSlaveProxyID, defaultExecutionTime, defaultErrorCode, defaultStatusVars, []byte(schema), []byte(query))
	if err != nil {
		return nil, terror.Annotatef(err, "generate QueryEvent for schema %s, query %s", schema, query)
	}
	latestPos = queryEv.Header.LogPos

	var buf bytes.Buffer
	var events []*replication.BinlogEvent
	if genGTID {
		_, err = buf.Write(gtidEv.RawData)
		if err != nil {
			return nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write GTIDEvent data % X", gtidEv.RawData)
		}
		events = append(events, gtidEv)
	}
	_, err = buf.Write(queryEv.RawData)
	if err != nil {
		return nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write QueryEvent data % X", queryEv.RawData)
	}
	events = append(events, queryEv)

	return &DDLDMLResult{
		Events:     events,
		Data:       buf.Bytes(),
		LatestPos:  latestPos,
		LatestGTID: latestGTID,
	}, nil
}

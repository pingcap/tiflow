// Copyright 2023 PingCAP, Inc.
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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type CanalJSONMessageWithExtension struct {
	ID        int64    `json:"id"`
	Schema    string   `json:"database"`
	Table     string   `json:"table"`
	PKNames   []string `json:"pkNames"`
	IsDDL     bool     `json:"isDdl"`
	EventType string   `json:"type"`
	// officially the timestamp of the event-time of the message, in milliseconds since Epoch.
	ExecutionTime int64 `json:"es"`
	// officially the timestamp of building the message, in milliseconds since Epoch.
	BuildTime int64 `json:"ts"`
	// SQL that generated the change event, DDL or Query
	Query string `json:"sql"`
	// only works for INSERT / UPDATE / DELETE events, records each column's java representation type.
	SQLType map[string]int32 `json:"sqlType"`
	// only works for INSERT / UPDATE / DELETE events, records each column's mysql representation type.
	MySQLType map[string]string `json:"mysqlType"`
	// A Datum should be a string or nil
	Data []map[string]interface{} `json:"data"`
	Old  []map[string]interface{} `json:"old"`

	// Extensions is a TiCDC custom field that different from official Canal-JSON format.
	// It would be useful to store something for special usage.
	// At the moment, only store the `tso` of each event,
	// which is useful if the message consumer needs to restore the original transactions.
	Extensions *tidbExtension `json:"_tidb"`
}

type tidbExtension struct {
	CommitTs           uint64 `json:"commitTs,omitempty"`
	WatermarkTs        uint64 `json:"watermarkTs,omitempty"`
	OnlyHandleKey      bool   `json:"onlyHandleKey,omitempty"`
	ClaimCheckLocation string `json:"claimCheckLocation,omitempty"`
}

func main() {
	var (
		kafkaAddr       = "127.0.0.1:9092"
		topic           = "canal-json-handle-key-only-example"
		consumerGroupID = "canal-json-handle-key-only-example-group"

		upstreamTiDBDSN = "root@tcp(127.0.0.1:4000)/?"
	)

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaAddr},
		GroupID: consumerGroupID,
		Topic:   topic,
	})
	defer consumer.Close()

	ctx := context.Background()

	upstreamTiDB, err := openDB(ctx, upstreamTiDBDSN)
	if err != nil {
		log.Error("open upstream TiDB failed", zap.String("dsn", upstreamTiDBDSN), zap.Error(err))
	}
	defer upstreamTiDB.Close()

	log.Info("start consuming ...", zap.String("kafka", kafkaAddr), zap.String("topic", topic), zap.String("groupID", consumerGroupID))
	for {
		rawMessage, err := consumer.FetchMessage(ctx)
		if err != nil {
			log.Error("read kafka message failed", zap.Error(err))
			break
		}

		var message CanalJSONMessageWithExtension
		err = json.Unmarshal(rawMessage.Value, &message)
		if err != nil {
			log.Error("unmarshal kafka message failed", zap.Error(err))
			break
		}

		if message.IsDDL {
			log.Info("DDL message received, skip",
				zap.String("DDL", message.Query))
			continue
		}

		if message.EventType == "TIDB_WATERMARK" {
			log.Info("TIDB_WATERMARK message received, skip")
			continue
		}

		if message.Extensions == nil {
			log.Info("cannot found the tidb extension part, skip",
				zap.String("message", string(rawMessage.Value)))
			continue
		}

		// handle key only message found, query the upstream TiDB to get the complete row.
		if message.Extensions.OnlyHandleKey {
			commitTs := message.Extensions.CommitTs
			schema := message.Schema
			table := message.Table

			switch message.EventType {
			case "INSERT":
				conditions := message.Data[0]
				allColumns, err := SnapshotQuery(ctx, upstreamTiDB, commitTs, schema, table, conditions)
				if err != nil {
					log.Error("snapshot query failed", zap.Error(err))
					break
				}

				row, err := formatRowValue(allColumns)
				if err != nil {
					log.Error("format row value failed", zap.Error(err))
					break
				}
				log.Info("newly inserted row found", zap.String("row", row))
			case "UPDATE":
				conditions := message.Data[0]
				allColumns, err := SnapshotQuery(ctx, upstreamTiDB, commitTs, schema, table, conditions)
				if err != nil {
					log.Error("snapshot query failed", zap.Error(err))
					break
				}

				row, err := formatRowValue(allColumns)
				if err != nil {
					log.Error("format row value failed", zap.Error(err))
					break
				}

				conditions = message.Old[0]
				commitTs := message.Extensions.CommitTs - 1
				allPreviousColumns, err := SnapshotQuery(ctx, upstreamTiDB, commitTs, schema, table, conditions)
				if err != nil {
					log.Error("snapshot query failed", zap.Error(err))
					break
				}

				oldRow, err := formatRowValue(allPreviousColumns)
				if err != nil {
					log.Error("format row value failed", zap.Error(err))
					break
				}
				log.Info("updated row found", zap.String("row", row), zap.String("oldRow", oldRow))
			case "DELETE":
				conditions := message.Data[0]
				commitTs := message.Extensions.CommitTs - 1
				allPreviousColumns, err := SnapshotQuery(ctx, upstreamTiDB, commitTs, schema, table, conditions)
				if err != nil {
					log.Error("snapshot query failed", zap.Error(err))
					break
				}

				oldRow, err := formatRowValue(allPreviousColumns)
				if err != nil {
					log.Error("format row value failed", zap.Error(err))
					break
				}
				log.Info("deleted row found", zap.String("oldRow", oldRow))
			}
		}

		if err := consumer.CommitMessages(ctx, rawMessage); err != nil {
			log.Error("commit kafka message failed", zap.Error(err))
			break
		}
	}
}

func formatRowValue(holder *columnsHolder) (string, error) {
	columns := make([]string, 0, holder.Length())
	for i := 0; i < holder.Length(); i++ {
		columnType := holder.Types[i]
		name := columnType.Name()
		mysqlType := strings.ToLower(columnType.DatabaseTypeName())

		var value string
		rawValue := holder.Values[i].([]uint8)
		if strings.Contains(mysqlType, "bit") || strings.Contains(mysqlType, "set") {
			bitValue, err := binaryLiteralToInt(rawValue)
			if err != nil {
				return "", err
			}
			value = strconv.FormatUint(bitValue, 10)
		} else {
			value = string(rawValue)
		}

		columns = append(columns, fmt.Sprintf("%v = %v", name, value))
	}

	return strings.Join(columns, ","), nil
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

// columnsHolder read columns from sql.Rows
type columnsHolder struct {
	Values        []interface{}
	ValuePointers []interface{}
	Types         []*sql.ColumnType
}

func newColumnHolder(rows *sql.Rows) (*columnsHolder, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columnTypes))
	valuePointers := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	return &columnsHolder{
		Values:        values,
		ValuePointers: valuePointers,
		Types:         columnTypes,
	}, nil
}

// Length return the column count
func (h *columnsHolder) Length() int {
	return len(h.Values)
}

// SnapshotQuery query the db by the snapshot read with the given commitTs
func SnapshotQuery(
	ctx context.Context, db *sql.DB, commitTs uint64, schema, table string, conditions map[string]interface{},
) (*columnsHolder, error) {
	// 1. set snapshot read
	query := fmt.Sprintf("set @@tidb_snapshot=%d", commitTs)
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Error("establish connection to the upstream tidb failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
		return nil, err
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, query)
	if err != nil {
		mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
		if ok {
			// Error 8055 (HY000): snapshot is older than GC safe point
			if mysqlErr.Number == 8055 {
				log.Error("set snapshot read failed, since snapshot is older than GC safe point")
			}
		}

		log.Error("set snapshot read failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
		return nil, err
	}

	// 2. query the whole row
	query = fmt.Sprintf("select * from `%s`.`%s` where ", schema, table)
	var whereClause string
	for name, value := range conditions {
		if whereClause != "" {
			whereClause += " and "
		}
		whereClause += fmt.Sprintf("`%s` = '%v'", name, value)
	}
	query += whereClause

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		log.Error("query row failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	holder, err := newColumnHolder(rows)
	if err != nil {
		log.Error("obtain the columns holder failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
		return nil, err
	}
	for rows.Next() {
		err = rows.Scan(holder.ValuePointers...)
		if err != nil {
			log.Error("scan row failed",
				zap.String("schema", schema), zap.String("table", table),
				zap.Uint64("commitTs", commitTs), zap.Error(err))
			return nil, err
		}
	}

	return holder, nil
}

// binaryLiteralToInt convert bytes into uint64,
// by follow https://github.com/pingcap/tidb/blob/e3417913f58cdd5a136259b902bf177eaf3aa637/types/binary_literal.go#L105
func binaryLiteralToInt(bytes []byte) (uint64, error) {
	bytes = trimLeadingZeroBytes(bytes)
	length := len(bytes)

	if length > 8 {
		log.Error("invalid bit value found", zap.ByteString("value", bytes))
		return math.MaxUint64, errors.New("invalid bit value")
	}

	if length == 0 {
		return 0, nil
	}

	// Note: the byte-order is BigEndian.
	val := uint64(bytes[0])
	for i := 1; i < length; i++ {
		val = (val << 8) | uint64(bytes[i])
	}
	return val, nil
}

func trimLeadingZeroBytes(bytes []byte) []byte {
	if len(bytes) == 0 {
		return bytes
	}
	pos, posMax := 0, len(bytes)-1
	for ; pos < posMax; pos++ {
		if bytes[pos] != 0 {
			break
		}
	}
	return bytes[pos:]
}

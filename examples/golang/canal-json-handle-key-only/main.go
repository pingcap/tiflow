package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func main() {
	var (
		kafkaAddr       = "127.0.0.1:9092"
		topic           = "canal-json-handle-key-only"
		consumerGroupID = "canal-json-handle-key-only-group"

		upstreamTiDBDSN = "root@tcp(127.0.0.1:4000)/?"
	)

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaAddr},
		GroupID:  consumerGroupID,
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
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
		message, err := consumer.FetchMessage(ctx)
		if err != nil {
			log.Error("read kafka message failed", zap.Error(err))
			break
		}

		holder := make(map[string]interface{})
		err = json.Unmarshal(message.Value, &holder)
		if err != nil {
			log.Error("unmarshal kafka message failed", zap.Error(err))
			break
		}

		_, ok := holder["isDdl"]
		if ok {
			log.Info("DDL message received, skip", zap.String("message", string(message.Value)))
			continue
		}
		if eventType, ok := holder["type"]; ok && eventType == "TIDB_WATERMARK" {
			log.Info("TIDB_WATERMARK message received, skip", zap.String("message", string(message.Value)))
			continue
		}

		extensions, ok := holder["_tidb"]
		if !ok {
			log.Info("cannot found the tidb extension part, skip", zap.String("message", string(message.Value)))
			continue
		}

		_, ok = extensions.(map[string]interface{})["onlyHandleKey"]
		if ok {
			// commitTs := extensions.(map[string]interface{})["commitTs"].(uint64)

			// // allColumns, err := SnapshotQuery(ctx, upstreamTiDB, commitTs, holder[""])
		}

		if err := consumer.CommitMessages(ctx, message); err != nil {
			log.Error("commit kafka message failed", zap.Error(err))
			break
		}
	}
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

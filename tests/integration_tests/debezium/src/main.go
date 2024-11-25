// Copyright 2024 PingCAP, Inc.
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
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var (
	dbName      *string
	testCaseDir *string
	kafkaAddr   *string
)

var (
	dbMySQL *DBHelper
	dbTiDB  *DBHelper

	readerDebezium *kafka.Reader
	readerTiCDC    *kafka.Reader
)

type Kind string

const (
	KindMySQL Kind = "mysql"
	KindTiDB  Kind = "tidb"
)

func prepareDBConn(kind Kind, connString string) *DBHelper {
	db := NewDBHelper(kind)
	db.MustOpen(connString, "")
	return db
}

func prepareKafkaConn(topic string) *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       []string{*kafkaAddr},
		Topic:         topic,
		MaxBytes:      10e6, // 10MB
		RetentionTime: time.Hour,

		// Config below ensures we will not read history messages.
		GroupID:     uuid.New().String(),
		StartOffset: kafka.LastOffset,
	})
	return r
}

func buildDefaultDBConnStr(port int) string {
	return fmt.Sprintf("root@tcp(127.0.0.1:%d)/{db}?allowNativePasswords=true", port)
}

func main() {
	dbConnMySQL := flag.String("db.mysql", buildDefaultDBConnStr(3310), "The connection string to connect to a MySQL instance")
	dbConnTiDB := flag.String("db.tidb", buildDefaultDBConnStr(4000), "The connection string to connect to a TiDB instance")
	kafkaAddr = flag.String("cdc.kafka", "127.0.0.1:9092", "")
	topicDebezium := flag.String("cdc.topic.debezium", "output_debezium", "")
	topicTiCDC := flag.String("cdc.topic.ticdc", "output_ticdc", "")
	dbName = flag.String("db", "test", "The database to test with")
	testCaseDir = flag.String("dir", "./sql", "The directory of SQL test cases")

	flag.Parse()

	logger.Info("Info",
		zap.String("db.mysql", *dbConnMySQL),
		zap.String("db.tidb", *dbConnTiDB),
		zap.String("cdc.mysql", fmt.Sprintf("kafka://%s/%s", *kafkaAddr, *topicDebezium)),
		zap.String("cdc.tidb", fmt.Sprintf("kafka://%s/%s", *kafkaAddr, *topicTiCDC)),
	)

	readerDebezium = prepareKafkaConn(*topicDebezium)
	defer readerDebezium.Close()
	readerTiCDC = prepareKafkaConn(*topicTiCDC)
	defer readerTiCDC.Close()

	if !runAllTestCases(*testCaseDir, *dbConnMySQL, *dbConnTiDB) {
		os.Exit(1)
	}
}

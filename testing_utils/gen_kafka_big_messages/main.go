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

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/pingcap/errors"
)

// See: https://docs.pingcap.com/tidb/stable/tidb-limitations/#limitations-on-string-types
const varcharColumnMaxLen = 16383

// Value of col. Defined as a variable for testing.
var colValue = strings.Repeat("a", varcharColumnMaxLen)

type options struct {
	// The size of each row.
	// The default is 1MiB.
	// FIXME: Currently it does not have precise control over the size of each row.
	// The overhead needs to be calculated and processed accurately.
	rowBytes int
	// Total number of rows.
	// The default is 1 line.
	rowCount int
	// Sql file path.
	// The default is `./test.sql`.
	sqlFilePath string
	// Database name.
	// The default is `kafka_big_messages`.
	databaseName string
	// Table name.
	// The default is `test`.
	tableName string
}

func (o *options) validate() error {
	if o.rowBytes <= 0 {
		return errors.New("rowBytes must be greater than zero")
	}

	if o.rowCount <= 0 {
		return errors.New("rowCount must be greater than zero")
	}

	if o.sqlFilePath == "" {
		return errors.New("please specify the correct file path")
	}

	if o.databaseName == "" {
		return errors.New("please specify the database name")
	}

	if o.tableName == "" {
		return errors.New("please specify the table name")
	}

	return nil
}

func gatherOptions() *options {
	o := &options{}

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.IntVar(&o.rowBytes, "row-bytes", 1024*1024, "Number of bytes per row.")
	fs.IntVar(&o.rowCount, "row-count", 1, "Count of rows.")
	fs.StringVar(&o.sqlFilePath, "sql-file-path", "./test.sql", "Sql file path.")
	fs.StringVar(&o.databaseName, "database-name", "kafka_big_messages", "Database name.")
	fs.StringVar(&o.tableName, "table-name", "test", "Table name.")

	_ = fs.Parse(os.Args[1:])
	return o
}

func main() {
	o := gatherOptions()
	if err := o.validate(); err != nil {
		log.Panicf("Invalid options: %v", err)
	}

	file, err := os.OpenFile(o.sqlFilePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Panicf("Open sql file failed: %v", err)
	}

	_, err = file.Write([]byte(genDatabaseSql(o.databaseName)))
	if err != nil {
		log.Panicf("Wirte create database sql failed: %v", err)
	}

	_, err = file.Write([]byte(genCreateTableSql(o.rowBytes, o.tableName)))
	if err != nil {
		log.Panicf("Wirte create table sql failed: %v", err)
	}

	for i := 0; i < o.rowCount; i++ {
		_, err = file.Write([]byte(genInsertSql(o.rowBytes, o.tableName, i)))
		if err != nil {
			log.Panicf("Wirte insert sql failed: %v", err)
		}
	}
}

func genDatabaseSql(databaseName string) string {
	return fmt.Sprintf(`DROP DATABASE IF EXISTS %s;
CREATE DATABASE %s;
USE %s;

`, databaseName, databaseName, databaseName)
}

func genCreateTableSql(rawBytes int, tableName string) string {
	var cols string

	for i := 0; i < rawBytes/varcharColumnMaxLen; i++ {
		cols = fmt.Sprintf("%s, a%d VARCHAR(%d)", cols, i, varcharColumnMaxLen)
	}

	return fmt.Sprintf("CREATE TABLE %s(id int primary key %s);\n", tableName, cols)
}

func genInsertSql(rawBytes int, tableName string, id int) string {
	var values string

	for i := 0; i < rawBytes/varcharColumnMaxLen; i++ {
		values = fmt.Sprintf("%s, '%s'", values, colValue)
	}

	return fmt.Sprintf("INSERT INTO %s VALUES (%d%s);\n", tableName, id, values)
}

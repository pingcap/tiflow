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

package loader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb/parser/ast"
	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	parserpkg "github.com/pingcap/tiflow/dm/pkg/parser"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

func bytes2str(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}

// poor man's parse insert stmtement code
// learn from tidb-lightning and refactor it as format of mydumper file
// https://github.com/maxbube/mydumper/blob/master/mydumper.c#L2853
// later let it a package.
func parseInsertStmt(sql []byte, table *tableInfo, columnMapping *cm.Mapping) ([][]string, error) {
	var s, e, size int
	rows := make([][]string, 0, 1024)
	VALUES := []byte("VALUES")

	// If table has generated column, the dumped SQL file has a different `INSERT INTO` line,
	// which provides column names except generated column. such as following:
	//	INSERT INTO `t1` (`id`,`uid`,`name`,`info`) VALUES
	//	(1,10001,"Gabriel García Márquez",NULL),
	//	(2,10002,"Cien años de soledad",NULL);
	// otherwise dumped SQL file has content like following:
	//	INSERT INTO `t1` VALUES
	//	(1,"hello"),
	//	(2,"world");

	for {
		sql = sql[s:]
		size = len(sql)

		// seek start "("
		s = bytes.IndexByte(sql, '(')
		if s < 0 {
			break
		}

		// seek one line, it's one row
		for e = s + 3; e < size; e++ {
			if sql[e] == '\n' && (sql[e-1] == ',' || sql[e-1] == ';') && sql[e-2] == ')' {
				break
			}
			if sql[e] == '\n' && e-6 > s && bytes.Equal(sql[e-6:e], VALUES) {
				s = e + 1
				continue
			}
		}
		if e == size {
			return nil, terror.ErrLoadUnitInvalidFileEnding.Generate()
		}

		rp := e - 2
		// extract columns' values
		row, err := parseRowValues(sql[s+1:rp], table, columnMapping)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)

		s = e + 1
		if s >= size {
			break
		}
	}

	return rows, nil
}

func parseRowValues(str []byte, table *tableInfo, columnMapping *cm.Mapping) ([]string, error) {
	// values are separated by comma, but we can not split using comma directly
	// string is enclosed by single quote

	// a poor implementation, may be more robust later.
	values := make([]interface{}, 0, len(table.columnNameList))
	row := make([]string, 0, len(table.columnNameList))
	isChars := make([]byte, 0, len(table.columnNameList))
	size := len(str)
	var ch byte
	for i := 0; i < size; {
		ch = str[i]
		if ch == ' ' || ch == '\n' {
			i++
			continue
		}

		if ch != '\'' && ch != '"' {
			// no string, read until comma
			j := i + 1
			for ; j < size && str[j] != ','; j++ {
			}

			val := bytes.TrimSpace(str[i:j])

			values = append(values, bytes2str(val)) // ?? no need to trim ??
			isChars = append(isChars, 0x0)
			// skip ,
			i = j + 1
		} else {
			// read string until another single quote
			j := i + 1

			sch := ch
			for j < size {
				// nolint:gocritic
				if str[j] == '\\' {
					// skip escaped character
					j += 2
					continue
				} else if str[j] == sch {
					// matchup ending
					break
				} else {
					j++
				}
			}

			if j >= size {
				return nil, terror.ErrLoadUnitParseQuoteValues.Generate()
			}

			val := str[i+1 : j]
			values = append(values, bytes2str(val))
			isChars = append(isChars, sch)

			i = j + 2 // skip ' and ,
		}
	}

	if columnMapping != nil {
		cmValues, _, err := columnMapping.HandleRowValue(table.sourceSchema, table.sourceTable, table.columnNameList, values)
		if err != nil {
			return nil, terror.ErrLoadUnitDoColumnMapping.Delegate(err, values, table)
		}
		values = cmValues
	}

	for i := range values {
		val, ok := values[i].(string)
		if !ok {
			panic(fmt.Sprintf("%v is not string", values[i]))
		}
		if isChars[i] != 0x0 {
			columnVal := make([]byte, 0, len(val)+2)
			columnVal = append(columnVal, isChars[i])
			columnVal = append(columnVal, val...)
			columnVal = append(columnVal, isChars[i])
			row = append(row, string(columnVal))
		} else {
			row = append(row, val)
		}
	}
	if len(table.extendCol) > 0 {
		for _, v := range table.extendVal {
			row = append(row, "'"+v+"'")
		}
	}
	return row, nil
}

// exportStatement returns schema structure in sqlFile.
func exportStatement(sqlFile string) ([]byte, error) {
	fd, err := os.Open(sqlFile)
	if err != nil {
		return nil, terror.ErrLoadUnitReadSchemaFile.Delegate(err, sqlFile)
	}
	defer fd.Close()

	br := bufio.NewReader(fd)
	f, err := os.Stat(sqlFile)
	if err != nil {
		return nil, terror.ErrLoadUnitReadSchemaFile.Delegate(err, sqlFile)
	}

	data := make([]byte, 0, f.Size()+1)
	buffer := make([]byte, 0, f.Size()+1)
	for {
		line, err := br.ReadString('\n')
		if errors.Cause(err) == io.EOF {
			break
		}

		line = strings.TrimSpace(line[:len(line)-1])
		if len(line) == 0 {
			continue
		}

		buffer = append(buffer, []byte(line)...)
		if buffer[len(buffer)-1] == ';' {
			statement := string(buffer)
			if !(strings.HasPrefix(statement, "/*") && strings.HasSuffix(statement, "*/;")) {
				data = append(data, buffer...)
			}
			buffer = buffer[:0]
		} else {
			buffer = append(buffer, '\n')
		}
	}

	return data, nil
}

func tableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", schema, table)
}

func parseTable(ctx *tcontext.Context, r *regexprrouter.RouteTable, schema, table, file, sqlMode, sourceID string) (*tableInfo, error) {
	statement, err := exportStatement(file)
	if err != nil {
		return nil, err
	}

	parser2, err := utils.GetParserFromSQLModeStr(sqlMode)
	if err != nil {
		return nil, err
	}

	stmts, err := parserpkg.Parse(parser2, string(statement), "", "")
	if err != nil {
		return nil, terror.ErrLoadUnitParseStatement.Delegate(err, statement)
	}

	var (
		ct                 *ast.CreateTableStmt
		hasCreateTableStmt bool
	)
	for _, stmt := range stmts {
		ct, hasCreateTableStmt = stmt.(*ast.CreateTableStmt)
		if hasCreateTableStmt {
			break
		}
	}
	if !hasCreateTableStmt {
		return nil, terror.ErrLoadUnitNotCreateTable.Generate(statement, schema, table)
	}

	var (
		columns          = make([]string, 0, len(ct.Cols))
		hasGeneragedCols = false
		columnNameFields = ""
	)
	for _, col := range ct.Cols {
		skip := false
		for _, opt := range col.Options {
			if opt.Tp == ast.ColumnOptionGenerated {
				hasGeneragedCols = true
				skip = true
				break
			}
		}
		if !skip {
			columns = append(columns, col.Name.Name.O)
		}
	}
	extendCol, extendVal := r.FetchExtendColumn(schema, table, sourceID)
	if len(extendCol) > 0 {
		columns = append(columns, extendCol...)
	}
	if hasGeneragedCols {
		var escapeColumns []string
		for _, column := range columns {
			escapeColumns = append(escapeColumns, fmt.Sprintf("`%s`", column))
		}
		columnNameFields = "(" + strings.Join(escapeColumns, ",") + ") "
	}

	dstSchema, dstTable := fetchMatchedLiteral(ctx, r, schema, table)
	return &tableInfo{
		sourceSchema:   schema,
		sourceTable:    table,
		targetSchema:   dstSchema,
		targetTable:    dstTable,
		columnNameList: columns,
		insertHeadStmt: fmt.Sprintf("INSERT INTO `%s` %sVALUES", dstTable, columnNameFields),
		extendCol:      extendCol,
		extendVal:      extendVal,
	}, nil
}

// refine it later.
func reassemble(data []byte, table *tableInfo, columnMapping *cm.Mapping) (string, error) {
	rows, err := parseInsertStmt(data, table, columnMapping)
	if err != nil {
		return "", err
	}

	query := bytes.NewBuffer(make([]byte, 0, len(data)))
	fmt.Fprint(query, table.insertHeadStmt)
	seq := ","

	for i, row := range rows {
		if i == len(rows)-1 {
			seq = ";"
		}

		fmt.Fprintf(query, "(%s)%s", strings.Join(row, ","), seq)
	}

	return query.String(), nil
}

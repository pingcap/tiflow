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

package utils

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"go.uber.org/zap"
	"golang.org/x/net/http/httpproxy"

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

var (
	// OsExit is function placeholder for os.Exit.
	OsExit func(int)

	passwordPatterns = `(password: (\\")?)(.*?)((\\")?\\n)`
	sslPatterns      = `(ssl-(ca|key|cert)-bytes:)((\\n\s{4}-\s\d+)+)`

	passwordRegexp *regexp.Regexp
	sslRegexp      *regexp.Regexp
)

func init() {
	OsExit = os.Exit
	passwordRegexp = regexp.MustCompile(passwordPatterns)
	sslRegexp = regexp.MustCompile(sslPatterns)
	pb.HideSensitiveFunc = HideSensitive
}

// DecodeBinlogPosition parses a mysql.Position from string format.
func DecodeBinlogPosition(pos string) (*mysql.Position, error) {
	if len(pos) < 3 {
		return nil, terror.ErrInvalidBinlogPosStr.Generate(pos)
	}
	if pos[0] != '(' || pos[len(pos)-1] != ')' {
		return nil, terror.ErrInvalidBinlogPosStr.Generate(pos)
	}
	sp := strings.Split(pos[1:len(pos)-1], ",")
	if len(sp) != 2 {
		return nil, terror.ErrInvalidBinlogPosStr.Generate(pos)
	}
	position, err := strconv.ParseUint(strings.TrimSpace(sp[1]), 10, 32)
	if err != nil {
		return nil, terror.ErrInvalidBinlogPosStr.Delegate(err, pos)
	}
	return &mysql.Position{
		Name: strings.TrimSpace(sp[0]),
		Pos:  uint32(position),
	}, nil
}

// WaitSomething waits for something done with `true`.
func WaitSomething(backoff int, waitTime time.Duration, fn func() bool) bool {
	for i := 0; i < backoff; i++ {
		if fn() {
			return true
		}

		time.Sleep(waitTime)
	}

	return false
}

// IsContextCanceledError checks whether err is context.Canceled.
func IsContextCanceledError(err error) bool {
	return errors.Cause(err) == context.Canceled
}

// IgnoreErrorCheckpoint is used in checkpoint update.
func IgnoreErrorCheckpoint(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*gmysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case errno.ErrDupFieldName:
		return true
	default:
		return false
	}
}

// IsInterestedDDL checks whether sql is an interested ddl of dm.
// un-parsable ddl is always not interested.
func IsInterestedDDL(sql string, p *parser.Parser) bool {
	stmts, err := Parse(p, sql, "", "")
	if err != nil || len(stmts) == 0 {
		return false
	}
	return IsInterestedStmt(stmts[0])
}

// IsInterestedStmt whether stmt is an interested ddl of dm.
// including:
// - create/alter/drop database.
// - create/alter/drop/rename/truncate table (non-temporary).
// - create/drop index.
func IsInterestedStmt(stmt ast.StmtNode) bool {
	if _, ok := stmt.(ast.DDLNode); !ok {
		return false
	}
	switch n := stmt.(type) {
	case *ast.CreateTableStmt:
		return n.TemporaryKeyword == ast.TemporaryNone
	case *ast.CreateViewStmt:
		return false
	case *ast.DropTableStmt:
		return !n.IsView && n.TemporaryKeyword == ast.TemporaryNone
	}
	return true
}

// HideSensitive replace password with ******.
func HideSensitive(input string) string {
	output := passwordRegexp.ReplaceAllString(input, "$1******$4")
	output = sslRegexp.ReplaceAllString(output, "$1 \"******\"")
	return output
}

// UnwrapScheme removes http or https scheme from input.
func UnwrapScheme(s string) string {
	if strings.HasPrefix(s, "http://") {
		return s[len("http://"):]
	} else if strings.HasPrefix(s, "https://") {
		return s[len("https://"):]
	}
	return s
}

func wrapScheme(s string, https bool) string {
	if s == "" {
		return s
	}
	s = UnwrapScheme(s)
	if https {
		return "https://" + s
	}
	return "http://" + s
}

// WrapSchemes adds http or https scheme to input if missing. input could be a comma-separated list.
func WrapSchemes(s string, https bool) string {
	items := strings.Split(s, ",")
	output := make([]string, 0, len(items))
	for _, s := range items {
		output = append(output, wrapScheme(s, https))
	}
	return strings.Join(output, ",")
}

// WrapSchemesForInitialCluster acts like WrapSchemes, except input is "name=URL,...".
func WrapSchemesForInitialCluster(s string, https bool) string {
	items := strings.Split(s, ",")
	output := make([]string, 0, len(items))
	for _, item := range items {
		kv := strings.Split(item, "=")
		if len(kv) != 2 {
			output = append(output, item)
			continue
		}

		output = append(output, kv[0]+"="+wrapScheme(kv[1], https))
	}
	return strings.Join(output, ",")
}

// IsFakeRotateEvent return true if is this event is a fake rotate event
// If log pos equals zero then the received event is a fake rotate event and
// contains only a name of the next binlog file
// See https://github.com/mysql/mysql-server/blob/8e797a5d6eb3a87f16498edcb7261a75897babae/sql/rpl_binlog_sender.h#L235
// and https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/rpl_binlog_sender.cc#L899
func IsFakeRotateEvent(header *replication.EventHeader) bool {
	return header.Timestamp == 0 || header.LogPos == 0
}

// LogHTTPProxies logs HTTP proxy relative environment variables.
func LogHTTPProxies(useLogger bool) {
	if fields := proxyFields(); len(fields) > 0 {
		if useLogger {
			log.L().Warn("using proxy config", fields...)
		} else {
			filedsStr := make([]string, 0, len(fields))
			for _, field := range fields {
				filedsStr = append(filedsStr, field.Key+"="+field.String)
			}
			fmt.Printf("\n[Warning] [using proxy config] [%v]\n", strings.Join(filedsStr, ", "))
		}
	}
}

func proxyFields() []zap.Field {
	proxyCfg := httpproxy.FromEnvironment()
	fields := make([]zap.Field, 0, 3)
	if proxyCfg.HTTPProxy != "" {
		fields = append(fields, zap.String("http_proxy", proxyCfg.HTTPProxy))
	}
	if proxyCfg.HTTPSProxy != "" {
		fields = append(fields, zap.String("https_proxy", proxyCfg.HTTPSProxy))
	}
	if proxyCfg.NoProxy != "" {
		fields = append(fields, zap.String("no_proxy", proxyCfg.NoProxy))
	}
	return fields
}

// SetToSlice converts a map of struct{} value to a slice to pretty print.
func SetToSlice(set map[string]struct{}) []string {
	slice := make([]string, 0, len(set))
	for key := range set {
		slice = append(slice, key)
	}
	return slice
}

func NewStoppedTimer() *time.Timer {
	// stopped timer should be Reset with correct duration, so use 0 here
	t := time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	return t
}

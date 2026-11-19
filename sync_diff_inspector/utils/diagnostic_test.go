// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDiagnosticError(t *testing.T) {
	previous := dmlog.IsRedactLogEnabled()
	t.Cleanup(func() { dmlog.SetRedactLog(previous) })
	connectionErr := &net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4000}, Err: syscall.ECONNREFUSED}
	pathErr := &os.PathError{Op: "open", Path: "/tmp/missing-config.toml", Err: syscall.ENOENT}
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"connection", connectionErr, connectionErr.Error()},
		{"file", errors.Annotate(pathErr, "config private-literal"), pathErr.Error()},
		{"mixed wrappers", fmt.Errorf("SELECT 'private-literal': %w", errors.Trace(connectionErr)), connectionErr.Error()},
		{"url credentials", &url.Error{Op: "Get", URL: "http://user:private-password@127.0.0.1:4000", Err: connectionErr}, connectionErr.Error()},
		{"nested data", &os.PathError{Op: "open", Path: "/tmp/missing-config.toml", Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("SELECT 'private-literal'")}}, "open /tmp/missing-config.toml: dial tcp: details redacted"},
		{"context", errors.Annotate(context.Canceled, "SELECT 'private-literal'"), context.Canceled.Error()},
		{"deadline", context.DeadlineExceeded, context.DeadlineExceeded.Error()},
		{"eof", io.EOF, io.EOF.Error()},
		{"unexpected eof", io.ErrUnexpectedEOF, io.ErrUnexpectedEOF.Error()},
		{"sql", &mysql.MySQLError{Number: 1064, Message: "syntax error near 'private-literal'"}, "details redacted"},
		{"arbitrary", errors.New("invalid private-literal"), "details redacted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dmlog.SetRedactLog(true)
			require.Equal(t, tc.want, DiagnosticError(tc.err))
			dmlog.SetRedactLog(false)
			if tc.err != nil {
				require.Equal(t, tc.err.Error(), DiagnosticError(tc.err))
			} else {
				require.Empty(t, DiagnosticError(tc.err))
			}
		})
	}
}

func TestZapDiagnosticError(t *testing.T) {
	previous := dmlog.IsRedactLogEnabled()
	t.Cleanup(func() { dmlog.SetRedactLog(previous) })
	pathErr := &os.PathError{Op: "open", Path: "/tmp/report.txt", Err: syscall.EACCES}
	cases := []struct {
		name string
		err  error
		want zap.Field
	}{
		{"nil", nil, zap.Error(nil)},
		{"filesystem", errors.Annotate(pathErr, "private-literal"), zap.String("error", pathErr.Error())},
		{"sql", &mysql.MySQLError{Number: 1064, Message: "syntax error near 'private-literal'"}, zap.String("error", "details redacted")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dmlog.SetRedactLog(false)
			require.Equal(t, zap.Error(tc.err), ZapDiagnosticError(tc.err))
			dmlog.SetRedactLog(true)
			require.Equal(t, tc.want, ZapDiagnosticError(tc.err))
		})
	}
}

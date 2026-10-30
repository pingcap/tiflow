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
	stderrors "errors"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/pingcap/errors"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
)

// DiagnosticError keeps native connection and filesystem diagnostics useful when
// redaction is enabled, including addresses and paths, while hiding data-bearing
// errors and annotations such as SQL or configuration contents.
func DiagnosticError(err error) string {
	if err == nil {
		return ""
	}
	if !dmlog.IsRedactLogEnabled() {
		return err.Error()
	}
	for err != nil {
		switch native := err.(type) {
		case *net.OpError:
			diagnostic := *native
			diagnostic.Err = stderrors.New(DiagnosticError(native.Err))
			return diagnostic.Error()
		case *os.PathError:
			diagnostic := *native
			diagnostic.Err = stderrors.New(DiagnosticError(native.Err))
			return diagnostic.Error()
		case *os.LinkError:
			diagnostic := *native
			diagnostic.Err = stderrors.New(DiagnosticError(native.Err))
			return diagnostic.Error()
		case *os.SyscallError:
			diagnostic := *native
			diagnostic.Err = stderrors.New(DiagnosticError(native.Err))
			return diagnostic.Error()
		case *net.DNSError, syscall.Errno:
			return err.Error()
		}
		switch err {
		case context.Canceled, context.DeadlineExceeded, io.EOF, io.ErrUnexpectedEOF:
			return err.Error()
		}
		if cause := errors.Unwrap(err); cause != nil {
			err = cause
		} else {
			err = stderrors.Unwrap(err)
		}
	}
	return "details redacted"
}

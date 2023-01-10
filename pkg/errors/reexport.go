// Copyright 2022 PingCAP, Inc.
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

package errors

import (
	"errors"

	perrors "github.com/pingcap/errors"
)

var (
	// Is is a shortcut for errors.Is.
	Is = errors.Is
	// As is a shortcut for errors.As.
	As = errors.As
	// New is a shortcut for github.com/pingcap/errors.New.
	New = perrors.New
	// Errorf is a shortcut for github.com/pingcap/errors.Errorf.
	Errorf = perrors.Errorf
	// Trace is a shortcut for github.com/pingcap/errors.Trace.
	Trace = perrors.Trace
	// Cause is a shortcut for github.com/pingcap/errors.Cause.
	Cause = perrors.Cause
	// Annotate is a shortcut for github.com/pingcap/errors.Annotate.
	Annotate = perrors.Annotate
	// Annotatef is a shortcut for github.com/pingcap/errors.Annotatef.
	Annotatef = perrors.Annotatef
)

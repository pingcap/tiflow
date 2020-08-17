// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/errors"
)

// errors
var (
	// kv related errors
	ErrWriteTsConflict         = errors.Normalize("write ts conflict").SetErrCodeText("CDC:ErrWriteTsConflict")
	ErrChangeFeedNotExists     = errors.Normalize("changefeed not exists, key: %s").SetErrCodeText("CDC:ErrChangeFeedNotExists")
	ErrChangeFeedAlreadyExists = errors.Normalize("changefeed already exists, key: %s").SetErrCodeText("CDC:ErrChangeFeedAlreadyExists")
	ErrTaskStatusNotExists     = errors.Normalize("task status not exists, key: %s").SetErrCodeText("CDC:ErrTaskStatusNotExists")
	ErrTaskPositionNotExists   = errors.Normalize("task position not exists, key: %s").SetErrCodeText("CDC:ErrTaskPositionNotExists")
	ErrCaptureNotExist         = errors.Normalize("capture not exists, key: %s").SetErrCodeText("CDC:ErrCaptureNotExist")
	ErrGetAllStoresFailed      = errors.Normalize("get stores from pd failed").SetErrCodeText("CDC:ErrGetAllStoresFailed")

	// rule related errors
	ErrEncodeFailed      = errors.Normalize("encode failed: %s").SetErrCodeText("CDC:ErrEncodeFailed")
	ErrDecodeFailed      = errors.Normalize("decode failed: %s").SetErrCodeText("CDC:ErrDecodeFailed")
	ErrFilterRuleInvalid = errors.Normalize("filter rule is invalid").SetErrCodeText("CDC:ErrFilterRuleInvalid")

	// internal errors
	ErrAdminStopProcessor = errors.Normalize("stop processor by admin command").SetErrCodeText("CDC:ErrAdminStopProcessor")
	ErrUnresolved         = errors.Normalize("unresolved").SetErrCodeText("CDC:ErrUnresolved")
	// ErrVersionIncompatible is an error for running CDC on an incompatible Cluster.
	ErrVersionIncompatible   = errors.Normalize("version is incompatible: %s").SetErrCodeText("CDC:ErrVersionIncompatible")
	ErrCreateMarkTableFailed = errors.Normalize("create mark table failed").SetErrCodeText("CDC:ErrCreateMarkTableFailed")

	// sink related errors
	ErrExecDDLFailed     = errors.Normalize("exec DDL failed").SetErrCodeText("CDC:ErrExecDDLFailed")
	ErrorDDLEventIgnored = errors.Normalize("ddl event is ignored").SetErrCodeText("CDC:ErrorDDLEventIgnored")

	// utilities related errors
	ErrToTLSConfigFailed         = errors.Normalize("generate tls config failed").SetErrCodeText("CDC:ErrToTLSConfigFailed")
	ErrCheckClusterVersionFromPD = errors.Normalize("failed to request PD").SetErrCodeText("CDC:ErrCheckClusterVersionFromPD")
	ErrNewSemVersion             = errors.Normalize("create sem version").SetErrCodeText("CDC:ErrNewSemVersion")
	ErrCheckDirWritable          = errors.Normalize("check dir writable failed").SetErrCodeText("CDC:ErrCheckDirWritable")
	ErrLoadTimezone              = errors.Normalize("load timezone").SetErrCodeText("CDC:ErrLoadTimezone")
	ErrURLFormatInvalid          = errors.Normalize("url format is invalid").SetErrCodeText("CDC:ErrURLFormatInvalid")
)

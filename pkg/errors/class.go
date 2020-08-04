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

// Error classes
var (
	reg = errors.NewRegistry("CDC")

	// ClassServer is internal server error class
	ClassServer = reg.RegisterErrorClass(1, "server")
	// ClassEntry is KV event entry error class
	ClassEntry = reg.RegisterErrorClass(2, "entry")
	// ClassKV is etcd KV related error class
	ClassKV = reg.RegisterErrorClass(3, "kv")
	// ClassModel is data operation related error class
	ClassModel = reg.RegisterErrorClass(4, "model")
	// ClassPuller is puller error class
	ClassPuller = reg.RegisterErrorClass(5, "puller")
	// ClassSink is sink error class
	ClassSink = reg.RegisterErrorClass(6, "sink")
	// ClassUtil is basic utility error class
	ClassUtil = reg.RegisterErrorClass(7, "util")
	// ClassInternal is basic internal logic error class
	ClassInternal = reg.RegisterErrorClass(8, "internal")
)

// errors
var (
	ErrWriteTsConflict         = ClassKV.DefineError().TextualCode("ErrWriteTsConflict").MessageTemplate("write ts conflict").Build()
	ErrChangeFeedNotExists     = ClassKV.DefineError().TextualCode("ErrChangeFeedNotExists").MessageTemplate("changefeed not exists, key: %s").Build()
	ErrChangeFeedAlreadyExists = ClassKV.DefineError().TextualCode("ErrChangeFeedAlreadyExists").MessageTemplate("changefeed already exists, key: %s").Build()
	ErrTaskStatusNotExists     = ClassKV.DefineError().TextualCode("ErrTaskStatusNotExists").MessageTemplate("task status not exists, key: %s").Build()
	ErrTaskPositionNotExists   = ClassKV.DefineError().TextualCode("ErrTaskPositionNotExists").MessageTemplate("task position not exists, key: %s").Build()
	ErrCaptureNotExist         = ClassKV.DefineError().TextualCode("ErrCaptureNotExist").MessageTemplate("capture not exists, key: %s").Build()
	ErrGetAllStoresFailed      = ClassKV.DefineError().TextualCode("ErrGetAllStoresFailed").MessageTemplate("get stores from pd failed: %s").Build()

	ErrEncodeFailed      = ClassModel.DefineError().TextualCode("ErrEncodeFailed").MessageTemplate("encode failed: %s").Build()
	ErrDecodeFailed      = ClassModel.DefineError().TextualCode("ErrDecodeFailed").MessageTemplate("decode failed: %s").Build()
	ErrFilterRuleInvalid = ClassModel.DefineError().TextualCode("ErrFilterRuleInvalid").MessageTemplate("filter rule is invalid: %s").Build()

	ErrAdminStopProcessor = ClassInternal.DefineError().TextualCode("ErrAdminStopProcessor").MessageTemplate("stop processor by admin command").Build()
	ErrUnresolved         = ClassInternal.DefineError().TextualCode("ErrUnresolved").MessageTemplate("unresolved").Build()
	// ErrVersionIncompatible is an error for running CDC on an incompatible Cluster.
	ErrVersionIncompatible   = ClassInternal.DefineError().TextualCode("ErrVersionIncompatible").MessageTemplate("version is incompatible: %s").Build()
	ErrCreateMarkTableFailed = ClassInternal.DefineError().TextualCode("ErrCreateMarkTableFailed").MessageTemplate("create mark table failed: %s").Build()

	ErrExecDDLFailed     = ClassSink.DefineError().TextualCode("ErrExecDDLFailed").MessageTemplate("exec DDL failed").Build()
	ErrorDDLEventIgnored = ClassSink.DefineError().TextualCode("ErrorDDLEventIgnored").MessageTemplate("ddl event is ignored").Build()

	ErrToTLSConfigFailed         = ClassUtil.DefineError().TextualCode("ErrToTLSConfigFailed").MessageTemplate("generate tls config failed: %s").Build()
	ErrCheckClusterVersionFromPD = ClassUtil.DefineError().TextualCode("ErrCheckClusterVersionFromPD").MessageTemplate("failed to request PD: %s").Build()
	ErrNewSemVersion             = ClassUtil.DefineError().TextualCode("ErrNewSemVersion").MessageTemplate("create sem version: %s").Build()
	ErrCheckDirWritable          = ClassUtil.DefineError().TextualCode("ErrCheckDirWritable").MessageTemplate("check dir writable failed: %s").Build()
	ErrLoadTimezone              = ClassUtil.DefineError().TextualCode("ErrLoadTimezone").MessageTemplate("load timezone: %s").Build()
	ErrURLFormatInvalid          = ClassUtil.DefineError().TextualCode("ErrURLFormatInvalid").MessageTemplate("url format is invalid: %s").Build()
)

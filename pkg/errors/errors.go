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

package errors

import (
	"github.com/pingcap/errors"
)

// all dataflow engine errors
var (
	// This happens when a unknown executor send requests to master.
	ErrUnknownExecutorID        = errors.Normalize("cannot find executor ID: %d", errors.RFCCodeText("DFLOW:ErrUnknownExecutorID"))
	ErrTombstoneExecutor        = errors.Normalize("executor %d has been dead", errors.RFCCodeText("DFLOW:ErrTombstoneExecutor"))
	ErrSubJobFailed             = errors.Normalize("executor %d job %d", errors.RFCCodeText("DFLOW:ErrSubJobFailed"))
	ErrClusterResourceNotEnough = errors.Normalize("cluster resource is not enough, please scale out the cluster", errors.RFCCodeText("DFLOW:ErrClusterResourceNotEnough"))
	ErrBuildJobFailed           = errors.Normalize("", errors.RFCCodeText("DFLOW:ErrBuildJobFailed"))

	ErrExecutorDupRegister = errors.Normalize("executor %d has been registered", errors.RFCCodeText("DFLOW:ErrExecutorDupRegister"))
	ErrGrpcBuildConn       = errors.Normalize("dial grpc connection to %s failed", errors.RFCCodeText("DFLOW:ErrGrpcBuildConn"))
	ErrDecodeEtcdKeyFail   = errors.Normalize("failed to decode etcd key: %s", errors.RFCCodeText("DFLOW:ErrDecodeEtcdKeyFail"))

	// master related errors
	ErrMasterConfigParseFlagSet     = errors.Normalize("parse config flag set failed", errors.RFCCodeText("DFLOW:ErrMasterConfigParseFlagSet"))
	ErrMasterConfigInvalidFlag      = errors.Normalize("'%s' is an invalid flag", errors.RFCCodeText("DFLOW:ErrMasterConfigInvalidFlag"))
	ErrMasterDecodeConfigFile       = errors.Normalize("decode config file failed", errors.RFCCodeText("DFLOW:ErrMasterDecodeConfigFile"))
	ErrMasterConfigUnknownItem      = errors.Normalize("master config containes unknown configuration options: %s", errors.RFCCodeText("DFLOW:ErrMasterConfigUnknownItem"))
	ErrMasterGenEmbedEtcdConfigFail = errors.Normalize("", errors.RFCCodeText("DFLOW:ErrMasterGenEmbedEtcdConfigFail"))
	ErrMasterStartEmbedEtcdFail     = errors.Normalize("failed to start embed etcd", errors.RFCCodeText("DFLOW:ErrMasterStartEmbedEtcdFail"))
	ErrMasterParseURLFail           = errors.Normalize("failed to parse URL %s", errors.RFCCodeText("DFLOW:ErrMasterParseURLFail"))

	// executor related errors
	ErrExecutorConfigParseFlagSet = errors.Normalize("parse config flag set failed", errors.RFCCodeText("DFLOW:ErrExecutorConfigParseFlagSet"))
	ErrExecutorConfigInvalidFlag  = errors.Normalize("'%s' is an invalid flag", errors.RFCCodeText("DFLOW:ErrExecutorConfigInvalidFlag"))
	ErrExecutorDecodeConfigFile   = errors.Normalize("decode config file failed", errors.RFCCodeText("DFLOW:ErrExecutorDecodeConfigFile"))
	ErrExecutorConfigUnknownItem  = errors.Normalize("master config containes unknown configuration options: %s", errors.RFCCodeText("DFLOW:ErrExecutorConfigUnknownItem"))
	ErrHeartbeat                  = errors.Normalize("heartbeat error type: %s", errors.RFCCodeText("DFLOW:ErrHeartbeat"))
)

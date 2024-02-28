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

package validator

import (
	"context"
	"net/url"

	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/factory"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/pingcap/tiflow/pkg/util"
)

// Validate sink if given valid parameters.
// TODO: For now, we create a real sink instance and validate it.
// Maybe we should support the dry-run mode to validate sink.
func Validate(ctx context.Context,
	sinkURI string, cfg *config.ReplicaConfig,
	pdClock pdutil.Clock,
) error {
	uri, err := preCheckSinkURI(sinkURI)
	if err != nil {
		return err
	}

	if err := checkSyncPointSchemeCompatibility(uri, cfg); err != nil {
		return err
	}

	if cfg.BDRMode {
		err := checkBDRMode(ctx, uri, cfg)
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(contextutil.PutRoleInCtx(ctx, util.RoleClient))
	s, err := factory.New(ctx, sinkURI, cfg, make(chan error), pdClock)
	if err != nil {
		cancel()
		return err
	}
	cancel()
	s.Close()

	return nil
}

// checkSyncPointSchemeCompatibility checks if the sink scheme is compatible
// with the syncpoint feature.
func checkSyncPointSchemeCompatibility(
	uri *url.URL,
	cfg *config.ReplicaConfig,
) error {
	if cfg.EnableSyncPoint && !sink.IsMySQLCompatibleScheme(uri.Scheme) {
		return cerror.ErrSinkURIInvalid.
			GenWithStack(
				"sink uri scheme is not supported with syncpoint enabled"+
					"sink uri: %s", uri,
			)
	}
	return nil
}

// preCheckSinkURI do some pre-check for sink URI.
// 1. Check if sink URI is empty.
// 2. Check if we use correct IPv6 format in URI.(if needed)
func preCheckSinkURI(sinkURIStr string) (*url.URL, error) {
	if sinkURIStr == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStack("sink uri is empty")
	}

	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	// Check if we use the correct IPv6 address format.
	// Notice: We should not check the host name is empty or not,
	// because we have blackhole sink which has empty host name.
	// Also notice the host name different from host(host+port).
	if util.IsIPv6Address(sinkURI.Hostname()) &&
		!util.IsValidIPv6AddressFormatInURI(sinkURI.Host) {
		return nil, cerror.ErrSinkURIInvalid.GenWithStack("sink uri host is not valid IPv6 address, " +
			"when using IPv6 address in URI, please use [ipv6-address]:port")
	}

	return sinkURI, nil
}

func checkBDRMode(ctx context.Context, sinkURI *url.URL, replicaConfig *config.ReplicaConfig) error {
	maskSinkURI, err := util.MaskSinkURI(sinkURI.String())
	if err != nil {
		return err
	}

	if !sink.IsMySQLCompatibleScheme(sinkURI.Scheme) {
		return cerror.ErrSinkURIInvalid.
			GenWithStack("sink uri scheme is not supported in BDR mode, sink uri: %s", maskSinkURI)
	}
	cfg := pmysql.NewConfig()
	id := model.DefaultChangeFeedID("sink-verify")
	err = cfg.Apply(ctx, id, sinkURI, replicaConfig)
	if err != nil {
		return err
	}
	dsn, err := pmysql.GenBasicDSN(sinkURI, cfg)
	if err != nil {
		return err
	}
	testDB, err := pmysql.GetTestDB(ctx, dsn, pmysql.CreateMySQLDBConn)
	if err != nil {
		return err
	}
	defer testDB.Close()
	supported, err := pmysql.CheckIfBDRModeIsSupported(ctx, testDB)
	if err != nil {
		return err
	}
	if !supported {
		return cerror.ErrSinkURIInvalid.
			GenWithStack("downstream database does not support BDR mode, "+
				"please check your config, sink uri: %s", maskSinkURI)
	}
	return nil
}

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

package v2

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

// CDCMetaData returns all etcd key values used by cdc
func (h *OpenAPIV2) CDCMetaData(c *gin.Context) {
	kvs, err := h.capture.GetEtcdClient().GetAllCDCInfo(c)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp := make([]EtcdData, 0, len(kvs))
	for _, pair := range kvs {
		resp = append(resp, EtcdData{
			Key:   string(pair.Key),
			Value: string(pair.Value),
		})
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// ResolveLock resolve locks in regions
func (h *OpenAPIV2) ResolveLock(c *gin.Context) {
	var resolveLockReq ResolveLockReq
	if err := c.BindJSON(&resolveLockReq); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}
	var (
		err       error
		kvStorage tidbkv.Storage
	)
	if len(resolveLockReq.PDAddrs) > 0 {
		kvStorage, err = kv.CreateTiStore(strings.Join(resolveLockReq.PDAddrs, ","),
			&security.Credential{
				CAPath:   resolveLockReq.CAPath,
				CertPath: resolveLockReq.CertPath,
				KeyPath:  resolveLockReq.KeyPath,
			})
		if err != nil {
			_ = c.Error(err)
			return
		}
	} else {
		up, err := getCaptureDefaultUpstream(h.capture)
		if err != nil {
			_ = c.Error(err)
			return
		}
		kvStorage = up.KVStorage
	}

	if kvStorage == nil {
		c.Status(http.StatusServiceUnavailable)
		return
	}

	txnResolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage),
		model.DefaultChangeFeedID("changefeed-client"),
		util.RoleClient)
	err = txnResolver.Resolve(c, resolveLockReq.RegionID, resolveLockReq.Ts)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusNoContent)
}

// DeleteServiceGcSafePoint Delete CDC service GC safepoint in PD
func (h *OpenAPIV2) DeleteServiceGcSafePoint(c *gin.Context) {
	upstreamConfig := &UpstreamConfig{}
	if err := c.BindJSON(upstreamConfig); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	err := h.withUpstreamConfig(c, upstreamConfig,
		func(ctx context.Context, client pd.Client) error {
			err := gc.RemoveServiceGCSafepoint(c, client,
				h.capture.GetEtcdClient().GetGCServiceID())
			if err != nil {
				return cerror.WrapError(cerror.ErrInternalServerError, err)
			}
			return nil
		})
	if err != nil {
		_ = c.Error(err)
	}
	c.Status(http.StatusNoContent)
}

func (h *OpenAPIV2) withUpstreamConfig(c context.Context,
	upstreamConfig *UpstreamConfig,
	doWithClient func(ctx context.Context, client pd.Client) error,
) error {
	var (
		err      error
		pdClient pd.Client
	)
	if upstreamConfig.ID > 0 {
		upManager, err := h.capture.GetUpstreamManager()
		if err != nil {
			return errors.Trace(err)
		}
		up, ok := upManager.Get(upstreamConfig.ID)
		if !ok {
			return cerror.ErrUpstreamNotFound.GenWithStackByArgs(upstreamConfig.ID)
		}
		pdClient = up.PDClient
	} else if len(upstreamConfig.PDAddrs) > 0 {
		timeoutCtx, cancel := context.WithTimeout(c, 30*time.Second)
		defer cancel()
		pdClient, err = h.helpers.
			getPDClient(timeoutCtx, upstreamConfig.PDAddrs, &security.Credential{
				CAPath:        upstreamConfig.CAPath,
				CertPath:      upstreamConfig.CertPath,
				KeyPath:       upstreamConfig.KeyPath,
				CertAllowedCN: nil,
			})
		if err != nil {
			return cerror.WrapError(cerror.ErrInternalServerError, err)
		}
		defer pdClient.Close()
	} else {
		up, err := getCaptureDefaultUpstream(h.capture)
		if err != nil {
			return errors.Trace(err)
		}
		pdClient = up.PDClient
	}
	return doWithClient(c, pdClient)
}

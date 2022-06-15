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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/tikv"
)

// CDCMetaData returns all etcd key values used by cdc
func (h *OpenAPIV2) CDCMetaData(c *gin.Context) {
	kvs, err := h.capture.EtcdClient.GetAllCDCInfo(c)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp := make([]EtcdData, 0, len(kvs))
	for _, kv := range kvs {
		resp = append(resp, EtcdData{
			Key:   string(kv.Key),
			Value: string(kv.Value),
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
	up := h.capture.UpstreamManager.GetDefaultUpstream()
	defer up.Release()

	txnResolver := txnutil.NewLockerResolver(up.KVStorage.(tikv.Storage),
		model.DefaultChangeFeedID("changefeed-client"),
		util.RoleClient)
	err := txnResolver.Resolve(c, resolveLockReq.RegionID, resolveLockReq.Ts)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusNoContent)
}

// DeleteServiceGcSafePoint Delete CDC service GC safepoint in PD
func (h *OpenAPIV2) DeleteServiceGcSafePoint(c *gin.Context) {
	up := h.capture.UpstreamManager.GetDefaultUpstream()
	defer up.Release()
	err := gc.RemoveServiceGCSafepoint(c, up.PDClient, h.capture.EtcdClient.GetGCServiceID())
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusNoContent)
}

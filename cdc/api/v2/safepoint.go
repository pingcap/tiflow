// Copyright 2024 PingCAP, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// NOTE: This variable is a copy of github.com/tikv/pd/tools/pd-ctl/pdctl/command/gc_safepoint_command.
var (
	serviceGCSafepointPrefix = "/pd/api/v1/gc/safepoint"
	defaultServiceIDSuffix   = "-cdc-safepoint-suffix-"
)

func queryListServiceGCSafepoint(endpoint string) (ListServiceGCSafepoint, error) {
	var safepoint ListServiceGCSafepoint
	safepointURL, err := url.Parse(endpoint + serviceGCSafepointPrefix)
	if err != nil {
		return safepoint, err
	}
	netClient := &http.Client{}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, safepointURL.String(), http.NoBody)
	if err != nil {
		return safepoint, err
	}
	resp, err := netClient.Do(req)
	if err != nil {
		return safepoint, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var msg []byte
		msg, err = io.ReadAll(resp.Body)
		if err != nil {
			return safepoint, err
		}
		return safepoint, fmt.Errorf("failed to get response: \"[%d] %s", resp.StatusCode, msg)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return safepoint, err
	}
	if err := json.Unmarshal(content, &safepoint); err != nil {
		return safepoint, err
	}
	for _, s := range safepoint.ServiceGCSafepoints {
		s.ExpiredTime = time.Unix(s.ExpiredAt, 0).Format(time.RFC3339Nano)
	}
	return safepoint, nil
}

func (h *OpenAPIV2) getServiceID(serviceIDSuffix string) string {
	tag := defaultServiceIDSuffix + serviceIDSuffix
	return h.capture.GetEtcdClient().GetEnsureGCServiceID(tag)
}

func (h *OpenAPIV2) getPDSafepoint() (*SafePoint, error) {
	up, err := getCaptureDefaultUpstream(h.capture)
	if err != nil {
		return nil, err
	}
	return h.helpers.getPDSafepoint(up.PdEndpoints)
}

// querySafePoint request and returns safepoints from PD
// @Summary Get the safepoint information of a TiCDC node
// @Description This API is a synchronous interface. If the request is successful,
// the safepoint information of the corresponding pd is returned.
//
// @Tags common,v2
// @Produce json
// @Success 200 {object} SafePoint
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/safepoint [get]
func (h *OpenAPIV2) querySafePoint(c *gin.Context) {
	safePoint, err := h.getPDSafepoint()
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, safePoint)
}

// setSafePoint request and returns safepoints from PD
// @Summary Set the safepoint information of a TiCDC node
// @Description This API is a synchronous interface. If the request is successful,
// the safepoint information of the corresponding pd is returned.
//
// @Tags common,v2
// @Accept json
// @Produce json
// @Success 200 {object} SafePoint
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/safepoint [post]
func (h *OpenAPIV2) setSafePoint(c *gin.Context) {
	ctx := c.Request.Context()

	safePointConfig := &SafePointConfig{}
	if err := c.BindJSON(safePointConfig); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	if safePointConfig.TTL <= 0 {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, errors.New("can't set ttl<=0")))
		return
	}
	serviceID := h.getServiceID(safePointConfig.ServiceIDSuffix)

	resp := &SafePoint{}
	err := h.withUpstreamConfig(ctx, &UpstreamConfig{},
		func(ctx context.Context, client pd.Client) error {
			minServiceSafePoint, err := client.UpdateServiceGCSafePoint(ctx, serviceID, safePointConfig.TTL, safePointConfig.StartTs)
			if err != nil {
				return err
			}
			if minServiceSafePoint > safePointConfig.StartTs {
				// use minServiceSafePoint to set
				log.Warn("will use minServiceSafePoint to set safe point",
					zap.String("serviceId", serviceID),
					zap.Int64("ttl", safePointConfig.TTL),
					zap.Uint64("minServiceSafePoint", minServiceSafePoint),
					zap.Uint64("startTs", safePointConfig.StartTs),
				)
				minServiceSafePoint, err = client.UpdateServiceGCSafePoint(ctx, serviceID, safePointConfig.TTL, minServiceSafePoint)
				if err != nil {
					return cerror.WrapError(cerror.ErrCliInvalidServiceSafePoint, err, minServiceSafePoint, safePointConfig.StartTs)
				}
			}
			safePoint, err := h.getPDSafepoint()
			if err != nil {
				return err
			}
			resp = safePoint
			return nil
		})
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// deleteSafePoint request and returns safepoints from PD
// @Summary Delete the safepoint information of a TiCDC node
// @Description This API is a synchronous interface. If the request is successful,
// the safepoint information of the corresponding pd is returned.
//
// @Tags common,v2
// @Accept json
// @Produce json
// @Success 200 {object} SafePoint
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/safepoint [delete]
func (h *OpenAPIV2) deleteSafePoint(c *gin.Context) {
	ctx := c.Request.Context()

	safePointConfig := &SafePointConfig{}
	if err := c.BindJSON(safePointConfig); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	serviceID := h.getServiceID(safePointConfig.ServiceIDSuffix)

	resp := &SafePoint{}
	err := h.withUpstreamConfig(ctx, &UpstreamConfig{},
		func(ctx context.Context, client pd.Client) error {
			// set ttl equal zero
			minServiceSafePoint, err := client.UpdateServiceGCSafePoint(ctx, serviceID, 0, safePointConfig.StartTs)
			if err != nil {
				return err
			}
			if minServiceSafePoint > safePointConfig.StartTs {
				// use minServiceSafePoint to delete
				log.Warn("will use minServiceSafePoint to set safe point",
					zap.String("serviceId", serviceID),
					zap.Int64("ttl", safePointConfig.TTL),
					zap.Uint64("minServiceSafePoint", minServiceSafePoint),
					zap.Uint64("startTs", safePointConfig.StartTs),
				)
				minServiceSafePoint, err = client.UpdateServiceGCSafePoint(ctx, serviceID, 0, minServiceSafePoint)
				if err != nil {
					return cerror.WrapError(cerror.ErrCliInvalidServiceSafePoint, err, minServiceSafePoint, safePointConfig.StartTs)
				}
			}
			safePoint, err := h.getPDSafepoint()
			if err != nil {
				return err
			}
			resp = safePoint
			return nil
		})
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, resp)
}

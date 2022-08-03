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

package pdutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/retry"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	regionLabelPrefix     = "/pd/api/v1/config/region-label/rules"
	gcServiceSafePointURL = "/pd/api/v1/gc/safepoint"
	healthyAPI            = "/pd/api/v1/health"

	// Split the default rule by `6e000000000000000000f8` to keep metadata region
	// isolated from the normal data area.
	addMetaJSON = `{
		"sets": [
			{
				"id": "ticdc/meta",
				"labels": [
					{
						"key": "data-type",
						"value": "meta"
					}
				],
				"rule_type": "key-range",
				"data": [
					{
						"start_key": "6d00000000000000f8",
						"end_key": "6e00000000000000f8"
					}
				]
			}
		]
	}`
)

var defaultMaxRetry uint64 = 3

// pdAPIClient is api client of Placement Driver.
type pdAPIClient struct {
	pdClient   pd.Client
	dialClient *httputil.Client
}

// NewPDApiClient create a new pdAPIClient.
func NewPDApiClient(pdClient pd.Client, conf *config.SecurityConfig) (*pdAPIClient, error) {
	dialClient, err := httputil.NewClient(conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &pdAPIClient{
		pdClient:   pdClient,
		dialClient: dialClient,
	}, nil
}

// Close close idle http connections
func (pc *pdAPIClient) Close() {
	pc.dialClient.CloseIdleConnections()
}

// UpdateMetaLabel is a reentrant function that updates the meta-region label of upstream cluster.
func (pc *pdAPIClient) UpdateMetaLabel(ctx context.Context) error {
	err := retry.Do(ctx, func() error {
		err := pc.patchMetaLabel(ctx)
		if err != nil {
			log.Error("Fail to add meta region label to PD", zap.Error(err))
			return err
		}

		log.Info("Succeed to add meta region label to PD")
		return nil
	}, retry.WithMaxTries(defaultMaxRetry), retry.WithIsRetryableErr(func(err error) bool {
		switch errors.Cause(err) {
		case context.Canceled:
			return false
		}
		return true
	}))
	return err
}

// ServiceSafePoint contains gc service safe point
type ServiceSafePoint struct {
	ServiceID string `json:"service_id"`
	ExpiredAt int64  `json:"expired_at"`
	SafePoint uint64 `json:"safe_point"`
}

// ListServiceGCSafepoint is the response of pd list gc service safe point API
type ListServiceGCSafepoint struct {
	ServiceGCSafepoints []*ServiceSafePoint `json:"service_gc_safe_points"`
	GCSafePoint         uint64              `json:"gc_safe_point"`
}

// ListGcServiceSafePoint list gc service safepoint from PD
func (pc *pdAPIClient) ListGcServiceSafePoint(ctx context.Context) (*ListServiceGCSafepoint, error) {
	var (
		resp *ListServiceGCSafepoint
		err  error
	)
	err = retry.Do(ctx, func() error {
		resp, err = pc.listGcServiceSafePoint(ctx)
		if err != nil {
			return err
		}
		return nil
	}, retry.WithMaxTries(defaultMaxRetry), retry.WithIsRetryableErr(func(err error) bool {
		switch errors.Cause(err) {
		case context.Canceled:
			return false
		}
		return true
	}))
	return resp, err
}

func (pc *pdAPIClient) patchMetaLabel(ctx context.Context) error {
	url := pc.pdClient.GetLeaderAddr() + regionLabelPrefix
	header := http.Header{"Content-Type": {"application/json"}}
	content := []byte(addMetaJSON)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := pc.dialClient.DoRequest(ctx, url, http.MethodPatch,
		header, bytes.NewReader(content))
	return errors.Trace(err)
}

func (pc *pdAPIClient) listGcServiceSafePoint(
	ctx context.Context,
) (*ListServiceGCSafepoint, error) {
	url := pc.pdClient.GetLeaderAddr() + gcServiceSafePointURL

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	respData, err := pc.dialClient.DoRequest(ctx, url, http.MethodGet,
		nil, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := ListServiceGCSafepoint{}
	err = json.Unmarshal(respData, &resp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &resp, nil
}

// getAllMemberEndpoints return all pd members
func (pc *pdAPIClient) getAllMemberEndpoints(
	ctx context.Context,
) ([]*pdpb.Member, error) {
	members, err := pc.pdClient.GetAllMembers(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return members, nil
	//
	//result := make([]string, 0, len(members))
	//for _, m := range members {
	//	result = append(result, m.GetPeerUrls()[0])
	//}
	//return result, nil
}

func (pc *pdAPIClient) IsMemberHealthy(ctx context.Context) error {
	members, err := pc.getAllMemberEndpoints(ctx)
	if err != nil {
		return err
	}
	for _, m := range members {
		if err := pc.isMemberHealthy(ctx, m.GetPeerUrls()[0]); err != nil {
			return err
		}
	}
	return nil
}

func (pc *pdAPIClient) isMemberHealthy(ctx context.Context, endpoint string) error {
	url := endpoint + healthyAPI
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := pc.dialClient.Get(ctx, fmt.Sprintf("%s/", url))
	if err != nil {
		return err
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return nil
}

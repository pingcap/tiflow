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
	"net/http"
	"time"

	"github.com/pingcap/errors"
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

	// Split the default rule by following keys to keep metadata region isolated
	// from the normal data area.
	//
	// * `6e000000000000000000f8`, keys starts with "m".
	// * `748000fffffffffffffe00000000000000f8`, the table prefix of
	//   `tidb_ddl_job` table, which has the table ID 281474976710654,
	//   see "github.com/pingcap/tidb/ddl.JobTableID"
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
			},
			{
				"id": "ticdc/meta_tidb_ddl_job",
				"labels": [
					{
						"key": "data-type",
						"value": "meta"
					}
				],
				"rule_type": "key-range",
				"data": [
					{
						"start_key": "748000fffffffffffffe00000000000000f8",
						"end_key":   "748000ffffffffffffff00000000000000f8"
					}
				]
			}
		]
	}`
)

const (
	defaultMaxRetry       = 3
	defaultRequestTimeout = 5 * time.Second
)

// PDAPIClient is the api client of Placement Driver, include grpc client and http client.
type PDAPIClient struct {
	grpcClient pd.Client
	httpClient *httputil.Client
}

// NewPDAPIClient create a new pdAPIClient.
func NewPDAPIClient(pdClient pd.Client, conf *config.SecurityConfig) (*PDAPIClient, error) {
	dialClient, err := httputil.NewClient(conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &PDAPIClient{
		grpcClient: pdClient,
		httpClient: dialClient,
	}, nil
}

// Close the pd api client, at the moment only close idle http connections if there is any.
func (pc *PDAPIClient) Close() {
	pc.httpClient.CloseIdleConnections()
}

// UpdateMetaLabel is a reentrant function that updates the meta-region label of upstream cluster.
func (pc *PDAPIClient) UpdateMetaLabel(ctx context.Context) error {
	err := retry.Do(ctx, func() error {
		ctx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
		defer cancel()

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
func (pc *PDAPIClient) ListGcServiceSafePoint(
	ctx context.Context,
) (*ListServiceGCSafepoint, error) {
	var (
		resp *ListServiceGCSafepoint
		err  error
	)
	err = retry.Do(ctx, func() error {
		ctx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
		defer cancel()

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

func (pc *PDAPIClient) patchMetaLabel(ctx context.Context) error {
	url := pc.grpcClient.GetLeaderAddr() + regionLabelPrefix
	header := http.Header{"Content-Type": {"application/json"}}
	content := []byte(addMetaJSON)

	_, err := pc.httpClient.DoRequest(ctx, url, http.MethodPatch,
		header, bytes.NewReader(content))
	return errors.Trace(err)
}

func (pc *PDAPIClient) listGcServiceSafePoint(
	ctx context.Context,
) (*ListServiceGCSafepoint, error) {
	url := pc.grpcClient.GetLeaderAddr() + gcServiceSafePointURL

	respData, err := pc.httpClient.DoRequest(ctx, url, http.MethodGet,
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

// CollectMemberEndpoints return all members' endpoint
func (pc *PDAPIClient) CollectMemberEndpoints(ctx context.Context) ([]string, error) {
	members, err := pc.grpcClient.GetAllMembers(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result := make([]string, 0, len(members))
	for _, m := range members {
		clientUrls := m.GetClientUrls()
		if len(clientUrls) > 0 {
			result = append(result, clientUrls[0])
		}
	}
	return result, nil
}

// Healthy return error if the member corresponding to the endpoint is unhealthy
func (pc *PDAPIClient) Healthy(ctx context.Context, endpoint string) error {
	url := endpoint + healthyAPI
	resp, err := pc.httpClient.Get(ctx, fmt.Sprintf("%s/", url))
	if err != nil {
		return errors.Trace(err)
	}
	_ = resp.Body.Close()
	return nil
}

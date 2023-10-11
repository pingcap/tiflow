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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/spanz"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	regionLabelPrefix     = "/pd/api/v1/config/region-label/rules"
	gcServiceSafePointURL = "/pd/api/v1/gc/safepoint"
	healthyAPI            = "/pd/api/v1/health"
	scanRegionAPI         = "/pd/api/v1/regions/key"

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

// PDAPIClient is client for PD http API.
type PDAPIClient interface {
	UpdateMetaLabel(ctx context.Context) error
	ListGcServiceSafePoint(ctx context.Context) (*ListServiceGCSafepoint, error)
	CollectMemberEndpoints(ctx context.Context) ([]string, error)
	Healthy(ctx context.Context, endpoint string) error
	ScanRegions(ctx context.Context, span tablepb.Span) ([]RegionInfo, error)
	Close()
}

// pdAPIClient is the api client of Placement Driver, include grpc client and http client.
type pdAPIClient struct {
	grpcClient pd.Client
	httpClient *httputil.Client
}

// NewPDAPIClient create a new pdAPIClient.
func NewPDAPIClient(pdClient pd.Client, conf *security.Credential) (PDAPIClient, error) {
	dialClient, err := httputil.NewClient(conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &pdAPIClient{
		grpcClient: pdClient,
		httpClient: dialClient,
	}, nil
}

// Close the pd api client, at the moment only close idle http connections if there is any.
func (pc *pdAPIClient) Close() {
	pc.httpClient.CloseIdleConnections()
}

// UpdateMetaLabel is a reentrant function that updates the meta-region label of upstream cluster.
func (pc *pdAPIClient) UpdateMetaLabel(ctx context.Context) error {
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

// NewTestRegionInfo creates a new RegionInfo for test purpose.
func NewTestRegionInfo(regionID uint64, start, end []byte, writtenKeys uint64) RegionInfo {
	return RegionInfo{
		ID:          regionID,
		StartKey:    hex.EncodeToString(start),
		EndKey:      hex.EncodeToString(end),
		WrittenKeys: writtenKeys,
	}
}

// RegionInfo records detail region info for api usage.
// NOTE: This type is a copy of github.com/tikv/pd/server/api.RegionInfo.
// To reduce dependency tree, we do not import the api package directly.
type RegionInfo struct {
	ID          uint64 `json:"id"`
	StartKey    string `json:"start_key"`
	EndKey      string `json:"end_key"`
	WrittenKeys uint64 `json:"written_keys"`
}

// RegionsInfo contains some regions with the detailed region info.
// NOTE: This type is a copy of github.com/tikv/pd/server/api.RegionInfo.
// To reduce dependency tree, we do not import the api package directly.
type RegionsInfo struct {
	Count   int          `json:"count"`
	Regions []RegionInfo `json:"regions"`
}

// ScanRegions is a reentrant function that updates the meta-region label of upstream cluster.
func (pc *pdAPIClient) ScanRegions(ctx context.Context, span tablepb.Span) ([]RegionInfo, error) {
	scanLimit := 1024
	endpoints, err := pc.CollectMemberEndpoints(ctx)
	if err != nil {
		log.Warn("fail to collec pd member endpoints")
		return nil, errors.Trace(err)
	}
	return pc.scanRegions(ctx, span, endpoints, scanLimit)
}

func (pc *pdAPIClient) scanRegions(
	ctx context.Context, span tablepb.Span, endpoints []string, scanLimit int,
) ([]RegionInfo, error) {
	scan := func(endpoint string, startKey, endKey []byte) ([]RegionInfo, error) {
		query := url.Values{}
		query.Add("key", string(startKey))
		query.Add("end_key", string(endKey))
		query.Add("limit", strconv.Itoa(scanLimit))
		u, _ := url.Parse(endpoint + scanRegionAPI)
		u.RawQuery = query.Encode()
		resp, err := pc.httpClient.Get(ctx, u.String())
		if err != nil {
			log.Warn("fail to scan regions",
				zap.String("endpoint", endpoint), zap.Any("span", span))
			return nil, errors.Trace(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Warn("fail to scan regions",
				zap.String("endpoint", endpoint), zap.Any("span", span))
			return nil, errors.Trace(err)
		}
		regions := &RegionsInfo{}
		err = json.Unmarshal(data, regions)
		if err != nil {
			log.Warn("fail to scan regions",
				zap.String("endpoint", endpoint), zap.Any("span", span))
			return nil, errors.Trace(err)
		}
		return regions.Regions, nil
	}

	regions := []RegionInfo{}
	startKey := span.StartKey
	startKeyHex := strings.ToUpper(hex.EncodeToString(startKey))
	isFirstStartKey := true
	for spanz.EndCompare(startKey, span.EndKey) < 0 || (len(startKey) == 0 && isFirstStartKey) {
		for i, endpoint := range endpoints {
			r, err := scan(endpoint, startKey, span.EndKey)
			if err != nil && i+1 == len(endpoints) {
				return nil, errors.Trace(err)
			}

			if len(r) == 0 {
				// Because start key is less than end key, there must be some regions.
				log.Error("fail to scan region, missing region",
					zap.String("endpoint", endpoint))
				return nil, cerror.WrapError(cerror.ErrInternalServerError,
					fmt.Errorf("fail to scan region, missing region"))
			}
			if r[0].StartKey != startKeyHex {
				r[0].StartKey = strings.ToUpper(hex.EncodeToString(startKey))
				log.Info("start key mismatch, adjust start key",
					zap.String("startKey", startKeyHex),
					zap.String("regionStartKey", r[0].StartKey),
					zap.Uint64("regionID", r[0].ID))
			}
			regions = append(regions, r...)
			key, err := hex.DecodeString(regions[len(regions)-1].EndKey)
			if err != nil {
				log.Info("fail to decode region end key",
					zap.String("endKey", regions[len(regions)-1].EndKey),
					zap.Uint64("regionID", r[len(regions)-1].ID))
				return nil, errors.Trace(err)
			}
			startKey = tablepb.Key(key)
			startKeyHex = strings.ToUpper(hex.EncodeToString(startKey))
			isFirstStartKey = false
			break
		}
	}
	if regions[len(regions)-1].EndKey != string(span.EndKey) {
		regions[len(regions)-1].EndKey = strings.ToUpper(hex.EncodeToString(span.EndKey))
		log.Info("end key mismatch, adjust end key",
			zap.String("endKey", strings.ToUpper(hex.EncodeToString(span.EndKey))),
			zap.String("regionEndKey", regions[len(regions)-1].EndKey),
			zap.Uint64("regionID", regions[len(regions)-1].ID))
	}

	return regions, nil
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
func (pc *pdAPIClient) ListGcServiceSafePoint(
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

func (pc *pdAPIClient) patchMetaLabel(ctx context.Context) error {
	url := pc.grpcClient.GetLeaderAddr() + regionLabelPrefix
	header := http.Header{"Content-Type": {"application/json"}}
	content := []byte(addMetaJSON)

	_, err := pc.httpClient.DoRequest(ctx, url, http.MethodPatch,
		header, bytes.NewReader(content))
	return errors.Trace(err)
}

func (pc *pdAPIClient) listGcServiceSafePoint(
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
func (pc *pdAPIClient) CollectMemberEndpoints(ctx context.Context) ([]string, error) {
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
func (pc *pdAPIClient) Healthy(ctx context.Context, endpoint string) error {
	url := endpoint + healthyAPI
	resp, err := pc.httpClient.Get(ctx, fmt.Sprintf("%s/", url))
	if err != nil {
		return errors.Trace(err)
	}
	_ = resp.Body.Close()
	return nil
}

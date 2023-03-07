// Copyright 2023 PingCAP, Inc.
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

package main

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func testStatus(ctx context.Context, client *CDCRESTClient) error {
	resp := client.Get().WithURI("/status").Do(ctx)
	assertResponseIsOK(resp)
	if err := json.Unmarshal(resp.body, &ServerStatus{}); err != nil {
		log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
	}
	println("pass test: get status")
	return nil
}

func testClusterHealth(ctx context.Context, client *CDCRESTClient) error {
	resp := client.Get().WithURI("/health").Do(ctx)
	assertResponseIsOK(resp)
	assertEmptyResponseBody(resp)
	println("pass test: health")
	return nil
}

func testChangefeed(ctx context.Context, client *CDCRESTClient) error {
	//changefeed
	data := `{
		"changefeed_id": "changefeed-test-v2-black-hole-1",
		"sink_uri": "blackhole://",
		"replica_config":{
			"ignore_ineligible_table": true
		}
	}`
	resp := client.Post().
		WithBody(bytes.NewReader([]byte(data))).
		WithURI("/changefeeds").
		Do(ctx)
	assertResponseIsOK(resp)
	changefeedInfo1 := &ChangeFeedInfo{}
	if err := json.Unmarshal(resp.body, changefeedInfo1); err != nil {
		log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
	}

	//pause changefeed
	resp = client.Post().WithURI("changefeeds/changefeed-test-v2-black-hole-1/pause").Do(ctx)
	assertResponseIsOK(resp)
	assertEmptyResponseBody(resp)

	// check get changefeed
	cf := &ChangeFeedInfo{}
	for i := 0; i < 10; i++ {
		resp = client.Get().WithURI("changefeeds/changefeed-test-v2-black-hole-1").Do(ctx)
		assertResponseIsOK(resp)
		if err := json.Unmarshal(resp.body, cf); err != nil {
			log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
		}
		if cf.State == "stopped" {
			break
		}
	}
	if cf.State != "stopped" {
		log.Panic("pause changefeed failed", zap.Any("changefeed", cf))
	}
	// list changefeed
	resp = client.Get().WithURI("changefeeds?state=stopped").Do(ctx)
	assertResponseIsOK(resp)
	changefeedList := &ListResponse[ChangefeedCommonInfo]{}
	if err := json.Unmarshal(resp.body, changefeedList); err != nil {
		log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
	}
	if len(changefeedList.Items) != 1 {
		log.Panic("changefeed items is not equals to 1", zap.Any("list", changefeedList))
	}

	resp = client.Post().WithBody(bytes.NewReader(
		[]byte(`{"overwrite_checkpoint_ts":0}`))).
		WithURI("changefeeds/changefeed-test-v2-black-hole-1/resume").Do(ctx)
	assertResponseIsOK(resp)
	assertEmptyResponseBody(resp)

	// check get changefeed
	cf = &ChangeFeedInfo{}
	for i := 0; i < 10; i++ {
		resp = client.Get().WithURI("changefeeds/changefeed-test-v2-black-hole-1").Do(ctx)
		assertResponseIsOK(resp)
		if err := json.Unmarshal(resp.body, cf); err != nil {
			log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
		}
		if cf.State == "normal" {
			break
		}
	}
	if cf.State != "normal" {
		log.Panic("pause changefeed failed", zap.Any("changefeed", cf))
	}

	println("pass test: changefeed apis")
	return resp.err
}

func testSetLogLevel(ctx context.Context, client *CDCRESTClient) error {
	resp := client.Post().WithURI("/log").
		WithBody(&LogLevelReq{Level: "debug"}).
		Do(ctx)
	assertResponseIsOK(resp)
	assertEmptyResponseBody(resp)
	client.Post().WithURI("/log").
		WithBody(&LogLevelReq{Level: "info"}).
		Do(ctx)
	assertResponseIsOK(resp)
	assertEmptyResponseBody(resp)
	println("pass test: set log level")
	return nil
}

func assertEmptyResponseBody(resp *Result) {
	if "{}" != string(resp.body) {
		log.Panic("failed call api", zap.String("body", string(resp.body)))
	}
}

func assertResponseIsOK(resp *Result) {
	if resp.err != nil {
		log.Panic("failed call api", zap.Error(resp.Error()))
	}
	if resp.statusCode != 200 {
		log.Panic("api status code is not 200", zap.Int("code", resp.statusCode))
	}
}

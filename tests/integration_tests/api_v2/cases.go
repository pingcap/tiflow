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
	"reflect"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// customReplicaConfig some custom fake configs to test the compatibility
var customReplicaConfig = &ReplicaConfig{
	MemoryQuota:           1123450,
	CaseSensitive:         false,
	ForceReplicate:        false,
	IgnoreIneligibleTable: false,
	CheckGCSafePoint:      false,
	BDRMode:               util.AddressOf(false),
	EnableSyncPoint:       util.AddressOf(false),
	SyncPointInterval:     util.AddressOf(JSONDuration{duration: 10 * time.Minute}),
	SyncPointRetention:    util.AddressOf(JSONDuration{duration: 24 * time.Hour}),
	Filter: &FilterConfig{
		MySQLReplicationRules: &MySQLReplicationRules{
			DoTables:     []*Table{{"a", "b"}, {"c", "d"}},
			DoDBs:        []string{"a", "c"},
			IgnoreTables: []*Table{{"d", "e"}, {"f", "g"}},
			IgnoreDBs:    []string{"d", "x"},
		},
		IgnoreTxnStartTs: []uint64{1, 2, 3},
		EventFilters: []EventFilterRule{{
			Matcher:                  []string{"test.worker"},
			IgnoreEvent:              []string{"update"},
			IgnoreSQL:                []string{"^drop", "add column"},
			IgnoreInsertValueExpr:    "id >= 100",
			IgnoreUpdateNewValueExpr: "gender = 'male'",
			IgnoreUpdateOldValueExpr: "age < 18",
			IgnoreDeleteValueExpr:    "id > 100",
		}},
		Rules: []string{
			"a.d", "b.x",
		},
	},
	Mounter: &MounterConfig{
		WorkerNum: 17,
	},
	Sink: &SinkConfig{
		Protocol: "arvo",
		ColumnSelectors: []*ColumnSelector{
			{
				[]string{"a.b"},
				[]string{"c"},
			},
		},
		TxnAtomicity: "table",
		Terminator:   "a",
		CSVConfig: &CSVConfig{
			Quote:      string(config.DoubleQuoteChar),
			Delimiter:  config.Comma,
			NullString: config.NULL,
		},
		DateSeparator:            "day",
		EncoderConcurrency:       util.AddressOf(32),
		EnablePartitionSeparator: util.AddressOf(true),
		ContentCompatible:        util.AddressOf(true),
	},
	Scheduler: &ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: false,
		RegionThreshold:        13,
	},
	Integrity: &IntegrityConfig{
		IntegrityCheckLevel:   "none",
		CorruptionHandleLevel: "warn",
	},
	Consistent: &ConsistentConfig{
		Level:                 "none",
		MaxLogSize:            64,
		FlushIntervalInMs:     2000,
		MetaFlushIntervalInMs: 200,
		Storage:               "",
		UseFileBackend:        false,
		EncoderWorkerNum:      31,
		FlushWorkerNum:        18,
	},
}

// defaultReplicaConfig check if the default values is changed
var defaultReplicaConfig = &ReplicaConfig{
	MemoryQuota:        1024 * 1024 * 1024,
	CaseSensitive:      false,
	CheckGCSafePoint:   true,
	EnableSyncPoint:    util.AddressOf(false),
	SyncPointInterval:  util.AddressOf(JSONDuration{duration: 10 * time.Minute}),
	SyncPointRetention: util.AddressOf(JSONDuration{duration: 24 * time.Hour}),
	BDRMode:            util.AddressOf(false),
	Filter: &FilterConfig{
		Rules: []string{"*.*"},
	},
	Mounter: &MounterConfig{
		WorkerNum: 16,
	},
	Sink: &SinkConfig{
		CSVConfig: &CSVConfig{
			Quote:      string(config.DoubleQuoteChar),
			Delimiter:  config.Comma,
			NullString: config.NULL,
		},
		Terminator:               "\r\n",
		DateSeparator:            "day",
		EncoderConcurrency:       util.AddressOf(32),
		EnablePartitionSeparator: util.AddressOf(true),
		ContentCompatible:        util.AddressOf(false),
	},
	Scheduler: &ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: false,
		RegionThreshold:        100_000,
	},
	Integrity: &IntegrityConfig{
		IntegrityCheckLevel:   "none",
		CorruptionHandleLevel: "warn",
	},
	Consistent: &ConsistentConfig{
		Level:                 "none",
		MaxLogSize:            64,
		FlushIntervalInMs:     2000,
		MetaFlushIntervalInMs: 200,
		EncoderWorkerNum:      16,
		FlushWorkerNum:        8,
		Storage:               "",
		UseFileBackend:        false,
	},
}

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
	// changefeed with default value
	data := `{
		"changefeed_id": "changefeed-test-v2-black-hole-1",
		"sink_uri": "blackhole://",
        "namespace": "test"
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

	ensureChangefeed(ctx, client, changefeedInfo1.ID, "normal")
	resp = client.Get().WithURI("/changefeeds/" + changefeedInfo1.ID + "?namespace=test").Do(ctx)
	assertResponseIsOK(resp)
	cfInfo := &ChangeFeedInfo{}
	if err := json.Unmarshal(resp.body, cfInfo); err != nil {
		log.Panic("failed to unmarshal response", zap.String("body", string(resp.body)), zap.Error(err))
	}
	if !reflect.DeepEqual(cfInfo.Config, defaultReplicaConfig) {
		log.Panic("config is not equals",
			zap.Any("add", defaultReplicaConfig),
			zap.Any("get", cfInfo.Config))
	}

	// pause changefeed
	resp = client.Post().WithURI("changefeeds/changefeed-test-v2-black-hole-1/pause?namespace=test").Do(ctx)
	assertResponseIsOK(resp)
	assertEmptyResponseBody(resp)

	ensureChangefeed(ctx, client, changefeedInfo1.ID, "stopped")

	// update changefeed
	data = `{
		"sink_uri": "blackhole://?aa=bb",
		"replica_config":{
			"ignore_ineligible_table": true
		}
	}`
	resp = client.Put().
		WithBody(bytes.NewReader([]byte(data))).
		WithURI("/changefeeds/changefeed-test-v2-black-hole-1?namespace=test").
		Do(ctx)
	assertResponseIsOK(resp)
	changefeedInfo1 = &ChangeFeedInfo{}
	if err := json.Unmarshal(resp.body, changefeedInfo1); err != nil {
		log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
	}

	// update with full custom config
	newConfig := &ChangefeedConfig{
		ReplicaConfig: customReplicaConfig,
	}
	cdata, err := json.Marshal(newConfig)
	if err != nil {
		log.Panic("marshal failed", zap.Error(err))
	}
	resp = client.Put().
		WithBody(bytes.NewReader(cdata)).
		WithURI("/changefeeds/changefeed-test-v2-black-hole-1?namespace=test").
		Do(ctx)
	assertResponseIsOK(resp)

	resp = client.Get().WithURI("changefeeds/changefeed-test-v2-black-hole-1?namespace=test").Do(ctx)
	assertResponseIsOK(resp)
	cf := &ChangeFeedInfo{}
	if err := json.Unmarshal(resp.body, cf); err != nil {
		log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
	}
	if !reflect.DeepEqual(cf.Config, customReplicaConfig) {
		log.Panic("config is not equals",
			zap.Any("update", customReplicaConfig),
			zap.Any("get", cf.Config))
	}

	// list changefeed
	resp = client.Get().WithURI("changefeeds?state=stopped&namespace=test").Do(ctx)
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
		WithURI("changefeeds/changefeed-test-v2-black-hole-1/resume?namespace=test").Do(ctx)
	assertResponseIsOK(resp)
	assertEmptyResponseBody(resp)

	// check get changefeed
	ensureChangefeed(ctx, client, changefeedInfo1.ID, "normal")

	resp = client.Delete().
		WithURI("changefeeds/changefeed-test-v2-black-hole-1?namespace=test").Do(ctx)
	assertResponseIsOK(resp)
	assertEmptyResponseBody(resp)

	resp = client.Get().
		WithURI("changefeeds/changefeed-test-v2-black-hole-1?namespace=test").Do(ctx)
	if resp.statusCode == 200 {
		log.Panic("delete changefeed failed", zap.Any("resp", resp))
	}

	println("pass test: changefeed apis")
	return nil
}

func testCreateChangefeed(ctx context.Context, client *CDCRESTClient) error {
	config := ChangefeedConfig{
		ID:            "test-create-all",
		Namespace:     "test",
		SinkURI:       "blackhole://create=test",
		ReplicaConfig: customReplicaConfig,
	}
	resp := client.Post().
		WithBody(&config).
		WithURI("/changefeeds").
		Do(ctx)
	assertResponseIsOK(resp)
	ensureChangefeed(ctx, client, config.ID, "normal")
	resp = client.Get().WithURI("/changefeeds/" + config.ID + "?namespace=test").Do(ctx)
	assertResponseIsOK(resp)
	cfInfo := &ChangeFeedInfo{}
	if err := json.Unmarshal(resp.body, cfInfo); err != nil {
		log.Panic("failed to unmarshal response", zap.String("body", string(resp.body)), zap.Error(err))
	}
	if !reflect.DeepEqual(cfInfo.Config, config.ReplicaConfig) {
		log.Panic("config is not equals", zap.Any("add", config.ReplicaConfig), zap.Any("get", cfInfo.Config))
	}
	resp = client.Delete().WithURI("/changefeeds/" + config.ID + "?namespace=test").Do(ctx)
	assertResponseIsOK(resp)
	return nil
}

func testRemoveChangefeed(ctx context.Context, client *CDCRESTClient) error {
	resp := client.Delete().WithURI("changefeeds/changefeed-not-exist").Do(ctx)
	assertResponseIsOK(resp)
	println("pass test: delete changefeed apis")
	return nil
}

func testCapture(ctx context.Context, client *CDCRESTClient) error {
	resp := client.Get().WithURI("captures").Do(ctx)
	assertResponseIsOK(resp)
	captures := &ListResponse[Capture]{}
	if err := json.Unmarshal(resp.body, captures); err != nil {
		log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
	}
	if len(captures.Items) != 1 {
		log.Panic("capture size is not 1", zap.Any("resp", resp))
	}
	println("pass test: capture apis")
	return nil
}

func testProcessor(ctx context.Context, client *CDCRESTClient) error {
	resp := client.Get().WithURI("processors").Do(ctx)
	assertResponseIsOK(resp)
	processors := &ListResponse[ProcessorCommonInfo]{}
	if err := json.Unmarshal(resp.body, processors); err != nil {
		log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
	}
	if len(processors.Items) == 0 {
		log.Panic("processor size is 0", zap.Any("resp", resp))
	}

	processorDetail := &ProcessorDetail{}
	resp = client.Get().
		WithURI("processors/" + processors.Items[0].ChangeFeedID + "/" +
			processors.Items[0].CaptureID +
			"?namespace=" + processors.Items[0].Namespace).
		Do(ctx)
	assertResponseIsOK(resp)
	if err := json.Unmarshal(resp.body, processorDetail); err != nil {
		log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
	}
	println("pass test: processor apis")
	return nil
}

func testResignOwner(ctx context.Context, client *CDCRESTClient) error {
	resp := client.Post().WithURI("owner/resign").Do(ctx)
	assertResponseIsOK(resp)
	assertResponseIsOK(resp)
	println("pass test: owner apis")
	return nil
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

func ensureChangefeed(ctx context.Context, client *CDCRESTClient, id, state string) {
	var info *ChangeFeedInfo
	for i := 0; i < 10; i++ {
		resp := client.Get().
			WithURI("/changefeeds/" + id + "?namespace=test").Do(ctx)
		if resp.statusCode == 200 {
			info = &ChangeFeedInfo{}
			if err := json.Unmarshal(resp.body, info); err != nil {
				log.Panic("unmarshal failed", zap.String("body", string(resp.body)), zap.Error(err))
			}
			if info.State == state {
				return
			}
		}
		log.Info("check changefeed failed", zap.Int("time", i), zap.Any("info", info))
		time.Sleep(2 * time.Second)
	}
	log.Panic("ensure changefeed failed")
}

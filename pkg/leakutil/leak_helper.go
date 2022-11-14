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

package leakutil

import (
	"testing"

	"go.uber.org/goleak"
)

// defaultOpts is the default ignore list for goleak.
var defaultOpts = []goleak.Option{
	goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
	goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
	// library used by sarama, ref: https://github.com/rcrowley/go-metrics/pull/266
	goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"),
	// Because we close the sarama producer asynchronously, so we have to ignore these funcs.
	goleak.IgnoreTopFunction("github.com/Shopify/sarama.(*client).backgroundMetadataUpdater"),
	goleak.IgnoreTopFunction("github.com/Shopify/sarama.(*Broker).responseReceiver"),
	goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
}

// VerifyNone verifies that no unexpected leaks occur
// Note that this function is incompatible with `t.Parallel()`
func VerifyNone(t *testing.T, options ...goleak.Option) {
	options = append(options, defaultOpts...)
	goleak.VerifyNone(t, options...)
}

// SetUpLeakTest ignore unexpected common etcd and opencensus stack functions for goleak
// options can be used to implement other ignore items
func SetUpLeakTest(m *testing.M, options ...goleak.Option) {
	options = append(options, defaultOpts...)
	goleak.VerifyTestMain(m, options...)
}

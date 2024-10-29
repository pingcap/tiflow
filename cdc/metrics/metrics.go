// Copyright 2020 PingCAP, Inc.
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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// LagBucket returns the lag buckets for prometheus metric
// 10 seconds is the reasonable LAG for most cases,
// for prometheus histogram_quantile func,
// we use small bucket distance to do accurate approximation
func LagBucket() []float64 {
	// 0.5s-10s in step of 0.5s
	buckets := prometheus.LinearBuckets(0.5, 0.5, 20)
	// 11s-20s in step of 1s
	buckets = append(buckets, prometheus.LinearBuckets(11, 1, 10)...)
	// 21s-591s in step of 30s
	buckets = append(buckets, prometheus.LinearBuckets(21, 30, 20)...)
	// 900s-3600 in step of 300s
	buckets = append(buckets, prometheus.LinearBuckets(900, 300, 10)...)
	// [4000, 8000, 16000, 32000]
	buckets = append(buckets, prometheus.ExponentialBuckets(4000, 2, 4)...)
	return buckets
}

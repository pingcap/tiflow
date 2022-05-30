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

// Package pulsar provider a pulsar based mq Producer implementation.
//
// SinkURL format like:
// pulsar://{token}@{host}/{topic}?xx=xxx
//
// config options see links below:
// https://godoc.org/github.com/apache/pulsar-client-go/pulsar#ClientOptions
// https://godoc.org/github.com/apache/pulsar-client-go/pulsar#ProducerOptions
//
// Notice:
// 1. All option in url queries start with lowercase chars, e.g. `tlsAllowInsecureConnection`, `maxConnectionsPerBroker`.
// 2. Use `auth` to config authentication plugin type, `auth.*` to config auth params.
// See:
//   1. https://pulsar.apache.org/docs/en/reference-cli-tools/#pulsar-client
//   2. https://github.com/apache/pulsar-client-go/tree/master/pulsar/internal/auth
//
// For example:
// pulsar://{host}/{topic}?auth=token&auth.token={token}
package pulsar

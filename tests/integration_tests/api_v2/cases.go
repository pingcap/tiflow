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

import "context"

func testStatus(ctx context.Context, client *CDCRESTClient) error {
	resp := client.Get().WithURI("/status").Do(ctx)
	assertResponseIsOK(resp)
	println("pass test: get status")
	return nil
}

func testSetLogLevel(ctx context.Context, client *CDCRESTClient) error {
	resp := client.Post().WithURI("/log").
		WithBody(&LogLevelReq{Level: "debug"}).
		Do(ctx)
	assertResponseIsOK(resp)
	client.Post().WithURI("/log").
		WithBody(&LogLevelReq{Level: "info"}).
		Do(ctx)
	assertResponseIsOK(resp)
	println("pass test: set log level")
	return nil
}

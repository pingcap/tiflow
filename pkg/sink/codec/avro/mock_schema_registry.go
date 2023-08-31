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

package avro

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"

	"github.com/jarcoal/httpmock"
)

type mockConfluentRegistrySchema struct {
	content string
	version int
	ID      int
}

type mockRegistry struct {
	mu       sync.Mutex
	subjects map[string]*mockConfluentRegistrySchema
	newID    int
}

func startHTTPInterceptForTestingRegistry() {
	httpmock.Activate()

	registry := mockRegistry{
		subjects: make(map[string]*mockConfluentRegistrySchema),
		newID:    1,
	}

	httpmock.RegisterResponder(
		"GET",
		"http://127.0.0.1:8081",
		httpmock.NewStringResponder(200, "{}"),
	)

	httpmock.RegisterResponder("POST", `=~^http://127.0.0.1:8081/subjects/(.+)/versions`,
		func(req *http.Request) (*http.Response, error) {
			subject, err := httpmock.GetSubmatch(req, 1)
			if err != nil {
				return nil, err
			}
			reqBody, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			var reqData registerRequest
			err = json.Unmarshal(reqBody, &reqData)
			if err != nil {
				return nil, err
			}

			var respData registerResponse
			registry.mu.Lock()
			item, exists := registry.subjects[subject]
			if !exists {
				item = &mockConfluentRegistrySchema{
					content: reqData.Schema,
					version: 1,
					ID:      registry.newID,
				}
				registry.subjects[subject] = item
				respData.SchemaID = registry.newID
			} else {
				if item.content == reqData.Schema {
					respData.SchemaID = item.ID
				} else {
					item.content = reqData.Schema
					item.version++
					item.ID = registry.newID
					respData.SchemaID = registry.newID
				}
			}
			registry.newID++
			registry.mu.Unlock()
			return httpmock.NewJsonResponse(200, &respData)
		})

	httpmock.RegisterResponder("GET", `=~^http://127.0.0.1:8081/schemas/ids/(.+)`,
		func(req *http.Request) (*http.Response, error) {
			id, err := httpmock.GetSubmatchAsInt(req, 1)
			if err != nil {
				return httpmock.NewStringResponse(500, "Internal Server Error"), err
			}

			for key, item := range registry.subjects {
				if item.ID == int(id) {
					var respData lookupResponse
					respData.Schema = item.content
					respData.Name = key
					respData.SchemaID = item.ID
					return httpmock.NewJsonResponse(200, &respData)
				}
			}

			return httpmock.NewStringResponse(404, "Not Found"), nil
		})

	httpmock.RegisterResponder("DELETE", `=~^http://127.0.0.1:8081/subjects/(.+)`,
		func(req *http.Request) (*http.Response, error) {
			subject, err := httpmock.GetSubmatch(req, 1)
			if err != nil {
				return nil, err
			}

			registry.mu.Lock()
			defer registry.mu.Unlock()
			item, exists := registry.subjects[subject]
			if !exists {
				return httpmock.NewStringResponse(404, ""), nil
			}

			delete(registry.subjects, subject)
			// simplify the response not returning all the versions
			return httpmock.NewJsonResponse(200, []int{item.version})
		})

	failCounter := 0
	httpmock.RegisterResponder("POST", `=~^http://127.0.0.1:8081/may-fail`,
		func(req *http.Request) (*http.Response, error) {
			_, _ = io.ReadAll(req.Body)
			if failCounter < 3 {
				failCounter++
				return httpmock.NewStringResponse(500, ""), nil
			}
			return httpmock.NewStringResponse(200, ""), nil
		})
}

func stopHTTPInterceptForTestingRegistry() {
	httpmock.DeactivateAndReset()
}

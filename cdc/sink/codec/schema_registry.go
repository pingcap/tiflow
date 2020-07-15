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

package codec

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// AvroSchemaManager is used to register Avro Schemas to the Registry server,
// look up local cache according to the table's name, and fetch from the Registry
// in cache the local cache entry is missing.
type AvroSchemaManager struct {
	registryURL   string
	cache         map[string]*schemaCacheEntry
	subjectSuffix string
}

type schemaCacheEntry struct {
	tiSchemaID uint64
	registryID int
	codec      *goavro.Codec
}

type registerRequest struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

type registerResponse struct {
	ID int `json:"id"`
}

type lookupResponse struct {
	Name       string `json:"name"`
	RegistryID int    `json:"id"`
	Schema     string `json:"schema"`
}

// NewAvroSchemaManager creates a new AvroSchemaManager
func NewAvroSchemaManager(ctx context.Context, registryURL string, subjectSuffix string) (*AvroSchemaManager, error) {
	registryURL = strings.TrimRight(registryURL, "/")
	// Test connectivity to the Schema Registry
	// TODO TLS support
	req, err := http.NewRequest("GET", registryURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))

	if err != nil {
		return nil, errors.Annotate(err, "Test connection to Schema Registry failed")
	}
	defer resp.Body.Close()

	text, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Annotate(err, "Reading response from Schema Registry failed")
	}

	if string(text[:]) != "{}" {
		return nil, errors.New("Unexpected response from Schema Registry")
	}

	log.Info("Successfully tested connectivity to Schema Registry", zap.String("registryURL", registryURL))

	return &AvroSchemaManager{
		registryURL:   registryURL,
		cache:         make(map[string]*schemaCacheEntry, 1),
		subjectSuffix: subjectSuffix,
	}, nil
}

var regexRemoveSpaces = regexp.MustCompile(`\s`)

// Register the latest schema for a table to the Registry, by passing in a Codec
func (m *AvroSchemaManager) Register(ctx context.Context, tableName model.TableName, codec *goavro.Codec) error {
	// The Schema Registry expects the JSON to be without newline characters
	reqBody := registerRequest{
		Schema:     regexRemoveSpaces.ReplaceAllString(codec.Schema(), ""),
		SchemaType: "AVRO",
	}
	payload, err := json.Marshal(&reqBody)
	if err != nil {
		return errors.Annotate(err, "Could not marshal request to the Registry")
	}
	uri := m.registryURL + "/subjects/" + url.QueryEscape(m.tableNameToSchemaSubject(tableName)) + "/versions"
	log.Debug("Registering schema", zap.String("uri", uri), zap.ByteString("payload", payload))

	req, err := http.NewRequest("POST", uri, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json")
	resp, err := httpRetry(ctx, req, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Annotate(err, "Failed to read response from Registry")
	}

	if resp.StatusCode != 200 {
		log.Warn("Failed to register schema to the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.String("uri", uri),
			zap.ByteString("requestBody", payload),
			zap.ByteString("responseBody", body))
		return errors.New("Failed to register schema to the Registry, HTTP error")
	}

	var jsonResp registerResponse
	err = json.Unmarshal(body, &jsonResp)

	if err != nil {
		return errors.Annotate(err, "Failed to parse result from Registry")
	}

	if jsonResp.ID == 0 {
		return errors.Errorf("Illegal schema ID returned from Registry %d", jsonResp.ID)
	}

	log.Info("Registered schema successfully",
		zap.Int("id", jsonResp.ID),
		zap.String("uri", uri),
		zap.ByteString("body", body))

	return nil
}

// Lookup the latest schema and the Registry designated ID for that schema.
// TiSchemaId is only used to trigger fetching from the Registry server.
// Calling this method with a tiSchemaID other than that used last time will invariably trigger a RESTful request to the Registry.
// Returns (codec, registry schema ID, error)
func (m *AvroSchemaManager) Lookup(ctx context.Context, tableName model.TableName, tiSchemaID uint64) (*goavro.Codec, int, error) {
	key := m.tableNameToSchemaSubject(tableName)
	if entry, exists := m.cache[key]; exists && entry.tiSchemaID == tiSchemaID {
		log.Info("Avro schema lookup cache hit",
			zap.String("key", key),
			zap.Uint64("tiSchemaID", tiSchemaID),
			zap.Int("registryID", entry.registryID))
		return entry.codec, entry.registryID, nil
	}

	log.Info("Avro schema lookup cache miss",
		zap.String("key", key),
		zap.Uint64("tiSchemaID", tiSchemaID))

	uri := m.registryURL + "/subjects/" + url.QueryEscape(m.tableNameToSchemaSubject(tableName)) + "/versions/latest"
	log.Debug("Querying for latest schema", zap.String("uri", uri))

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, 0, errors.Annotate(err, "Error constructing request for Registry lookup")
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")

	resp, err := httpRetry(ctx, req, false)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, errors.Annotate(err, "Failed to read response from Registry")
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		log.Warn("Failed to query schema from the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.String("uri", uri),

			zap.ByteString("responseBody", body))
		return nil, 0, errors.New("Failed to query schema from the Registry, HTTP error")
	}

	if resp.StatusCode == 404 {
		log.Warn("Specified schema not found in Registry",
			zap.String("key", key),
			zap.Uint64("tiSchemaID", tiSchemaID))

		return nil, 0, errors.New("Schema not found in Registry")
	}

	var jsonResp lookupResponse
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, 0, errors.Annotate(err, "Failed to parse result from Registry")
	}

	cacheEntry := new(schemaCacheEntry)
	cacheEntry.codec, err = goavro.NewCodec(jsonResp.Schema)
	if err != nil {
		return nil, 0, errors.Annotate(err, "Creating Avro codec failed")
	}
	cacheEntry.registryID = jsonResp.RegistryID
	cacheEntry.tiSchemaID = tiSchemaID
	m.cache[m.tableNameToSchemaSubject(tableName)] = cacheEntry

	log.Info("Avro schema lookup successful with cache miss",
		zap.Uint64("tiSchemaID", cacheEntry.tiSchemaID),
		zap.Int("registryID", cacheEntry.registryID),
		zap.String("schema", cacheEntry.codec.Schema()))

	return cacheEntry.codec, cacheEntry.registryID, nil
}

// ClearRegistry clears the Registry subject for the given table. Should be idempotent.
// Exported for testing.
func (m *AvroSchemaManager) ClearRegistry(ctx context.Context, tableName model.TableName) error {
	uri := m.registryURL + "/subjects/" + url.QueryEscape(m.tableNameToSchemaSubject(tableName))
	req, err := http.NewRequest("DELETE", uri, nil)
	if err != nil {
		log.Error("Could not construct request for clearRegistry", zap.String("uri", uri))
		return err
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
	resp, err := httpRetry(ctx, req, true)
	if err != nil {
		return err
	}

	if resp.StatusCode == 200 {
		log.Info("Clearing Registry successful")
		return nil
	}

	if resp.StatusCode == 404 {
		log.Info("Registry already cleaned")
		return nil
	}

	log.Error("Error when clearing Registry", zap.Int("status", resp.StatusCode))
	return errors.Errorf("Error when clearing Registry, status = %d", resp.StatusCode)
}

func httpRetry(ctx context.Context, r *http.Request, allow404 bool) (*http.Response, error) {
	var (
		err  error
		resp *http.Response
	)

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxInterval = time.Second * 30
	for {
		resp, err = http.DefaultClient.Do(r.WithContext(ctx))

		if err != nil {
			log.Warn("HTTP request failed", zap.String("msg", err.Error()))
			goto checkCtx
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 || (resp.StatusCode == 404 && allow404) {
			break
		}
		log.Warn("HTTP server returned with error", zap.Int("status", resp.StatusCode))
		_ = resp.Body.Close()

	checkCtx:
		select {
		case <-ctx.Done():
			return nil, errors.New("HTTP retry cancelled")

		default:
		}

		time.Sleep(expBackoff.NextBackOff())
	}

	return resp, nil
}

func (m *AvroSchemaManager) tableNameToSchemaSubject(tableName model.TableName) string {
	// We should guarantee unique names for subjects
	return tableName.Schema + "." + tableName.Table + m.subjectSuffix
}

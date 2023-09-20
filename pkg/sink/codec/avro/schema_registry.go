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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

// schemaManager is used to register Avro Schemas to the Registry server,
// look up local cache according to the table's name, and fetch from the Registry
// in cache the local cache entry is missing.
type schemaManager struct {
	registryURL   string
	subjectSuffix string

	credential *security.Credential // placeholder, currently always nil

	cacheRWLock sync.RWMutex
	cache       map[string]*schemaCacheEntry
}

type schemaCacheEntry struct {
	tiSchemaID uint64
	registryID int
	codec      *goavro.Codec
}

type registerRequest struct {
	Schema string `json:"schema"`
	// Commented out for compatibility with Confluent 5.4.x
	// SchemaType string `json:"schemaType"`
}

type registerResponse struct {
	ID int `json:"id"`
}

type lookupResponse struct {
	Name       string `json:"name"`
	RegistryID int    `json:"id"`
	Schema     string `json:"schema"`
}

// NewAvroSchemaManager creates a new schemaManager and test connectivity to the schema registry
func NewAvroSchemaManager(
	ctx context.Context, credential *security.Credential, registryURL string, subjectSuffix string,
) (*schemaManager, error) {
	registryURL = strings.TrimRight(registryURL, "/")
	httpCli, err := httputil.NewClient(credential)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := httpCli.Get(ctx, registryURL)
	if err != nil {
		log.Error("Test connection to Schema Registry failed", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	defer resp.Body.Close()

	text, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("Reading response from Schema Registry failed", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	if string(text[:]) != "{}" {
		return nil, cerror.ErrAvroSchemaAPIError.GenWithStack(
			"Unexpected response from Schema Registry",
		)
	}

	log.Info(
		"Successfully tested connectivity to Schema Registry",
		zap.String("registryURL", registryURL),
	)

	return &schemaManager{
		registryURL:   registryURL,
		cache:         make(map[string]*schemaCacheEntry, 1),
		subjectSuffix: subjectSuffix,
	}, nil
}

// Register a schema in schema registry, no cache
func (m *schemaManager) Register(
	ctx context.Context,
	topicName string,
	codec *goavro.Codec,
) (int, error) {
	// The Schema Registry expects the JSON to be without newline characters
	buffer := new(bytes.Buffer)
	err := json.Compact(buffer, []byte(codec.Schema()))
	if err != nil {
		log.Error("Could not compact schema", zap.Error(err))
		return 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	reqBody := registerRequest{
		Schema: buffer.String(),
	}
	payload, err := json.Marshal(&reqBody)
	if err != nil {
		log.Error("Could not marshal request to the Registry", zap.Error(err))
		return 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	uri := m.registryURL + "/subjects/" + url.QueryEscape(
		m.topicNameToSchemaSubject(topicName),
	) + "/versions"
	log.Info("Registering schema", zap.String("uri", uri), zap.ByteString("payload", payload))

	req, err := http.NewRequestWithContext(ctx, "POST", uri, bytes.NewReader(payload))
	if err != nil {
		log.Error("Failed to NewRequestWithContext", zap.Error(err))
		return 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add(
		"Accept",
		"application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, "+
			"application/json",
	)
	req.Header.Add("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := httpRetry(ctx, m.credential, req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("Failed to read response from Registry", zap.Error(err))
		return 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	if resp.StatusCode != 200 {
		// https://docs.confluent.io/platform/current/schema-registry/develop/api.html \
		// #post--subjects-(string-%20subject)-versions
		// 409 for incompatible schema
		log.Error(
			"Failed to register schema to the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.String("uri", uri),
			zap.ByteString("requestBody", payload),
			zap.ByteString("responseBody", body),
		)
		return 0, cerror.ErrAvroSchemaAPIError.GenWithStackByArgs()
	}

	var jsonResp registerResponse
	err = json.Unmarshal(body, &jsonResp)

	if err != nil {
		log.Error("Failed to parse result from Registry", zap.Error(err))
		return 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	if jsonResp.ID == 0 {
		return 0, cerror.ErrAvroSchemaAPIError.GenWithStack(
			"Illegal schema ID returned from Registry %d",
			jsonResp.ID,
		)
	}

	log.Info("Registered schema successfully",
		zap.Int("id", jsonResp.ID),
		zap.String("uri", uri),
		zap.ByteString("body", body))

	return jsonResp.ID, nil
}

// Lookup the latest schema and the Registry designated ID for that schema.
// TiSchemaId is only used to trigger fetching from the Registry server.
// Calling this method with a tiSchemaID other than that used last time will invariably trigger a
// RESTful request to the Registry.
// Returns (codec, registry schema ID, error)
// NOT USED for now, reserved for future use.
func (m *schemaManager) Lookup(
	ctx context.Context,
	topicName string,
	tiSchemaID uint64,
) (*goavro.Codec, int, error) {
	key := m.topicNameToSchemaSubject(topicName)
	m.cacheRWLock.RLock()
	if entry, exists := m.cache[key]; exists && entry.tiSchemaID == tiSchemaID {
		log.Info("Avro schema lookup cache hit",
			zap.String("key", key),
			zap.Uint64("tiSchemaID", tiSchemaID),
			zap.Int("registryID", entry.registryID))
		m.cacheRWLock.RUnlock()
		return entry.codec, entry.registryID, nil
	}
	m.cacheRWLock.RUnlock()

	log.Info("Avro schema lookup cache miss",
		zap.String("key", key),
		zap.Uint64("tiSchemaID", tiSchemaID))

	uri := m.registryURL + "/subjects/" + url.QueryEscape(key) + "/versions/latest"
	log.Debug("Querying for latest schema", zap.String("uri", uri))

	req, err := http.NewRequestWithContext(ctx, "GET", uri, nil)
	if err != nil {
		log.Error("Error constructing request for Registry lookup", zap.Error(err))
		return nil, 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add(
		"Accept",
		"application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, "+
			"application/json",
	)

	resp, err := httpRetry(ctx, m.credential, req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("Failed to parse result from Registry", zap.Error(err))
		return nil, 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		log.Error("Failed to query schema from the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.String("uri", uri),
			zap.ByteString("responseBody", body))
		return nil, 0, cerror.ErrAvroSchemaAPIError.GenWithStack(
			"Failed to query schema from the Registry, HTTP error",
		)
	}

	if resp.StatusCode == 404 {
		log.Warn("Specified schema not found in Registry",
			zap.String("key", key),
			zap.Uint64("tiSchemaID", tiSchemaID))
		return nil, 0, cerror.ErrAvroSchemaAPIError.GenWithStackByArgs(
			"Schema not found in Registry",
		)
	}

	var jsonResp lookupResponse
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		log.Error("Failed to parse result from Registry", zap.Error(err))
		return nil, 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	cacheEntry := new(schemaCacheEntry)
	cacheEntry.codec, err = goavro.NewCodec(jsonResp.Schema)
	if err != nil {
		log.Error("Creating Avro codec failed", zap.Error(err))
		return nil, 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	cacheEntry.registryID = jsonResp.RegistryID
	cacheEntry.tiSchemaID = tiSchemaID

	m.cacheRWLock.Lock()
	m.cache[key] = cacheEntry
	m.cacheRWLock.Unlock()

	log.Info("Avro schema lookup successful with cache miss",
		zap.Uint64("tiSchemaID", cacheEntry.tiSchemaID),
		zap.Int("registryID", cacheEntry.registryID),
		zap.String("schema", cacheEntry.codec.Schema()))

	return cacheEntry.codec, cacheEntry.registryID, nil
}

// SchemaGenerator represents a function that returns an Avro schema in JSON.
// Used for lazy evaluation
type SchemaGenerator func() (string, error)

// GetCachedOrRegister checks if the suitable Avro schema has been cached.
// If not, a new schema is generated, registered and cached.
// Re-registering an existing schema shall return the same id(and version), so even if the
// cache is out-of-sync with schema registry, we could reload it.
func (m *schemaManager) GetCachedOrRegister(
	ctx context.Context,
	topicName string,
	tiSchemaID uint64,
	schemaGen SchemaGenerator,
) (*goavro.Codec, int, error) {
	key := m.topicNameToSchemaSubject(topicName)
	m.cacheRWLock.RLock()
	if entry, exists := m.cache[key]; exists && entry.tiSchemaID == tiSchemaID {
		log.Debug("Avro schema GetCachedOrRegister cache hit",
			zap.String("key", key),
			zap.Uint64("tiSchemaID", tiSchemaID),
			zap.Int("registryID", entry.registryID))
		m.cacheRWLock.RUnlock()
		return entry.codec, entry.registryID, nil
	}
	m.cacheRWLock.RUnlock()

	log.Info("Avro schema lookup cache miss",
		zap.String("key", key),
		zap.Uint64("tiSchemaID", tiSchemaID))

	schema, err := schemaGen()
	if err != nil {
		return nil, 0, err
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Error("GetCachedOrRegister: Could not make goavro codec", zap.Error(err))
		return nil, 0, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	id, err := m.Register(ctx, topicName, codec)
	if err != nil {
		log.Error("GetCachedOrRegister: Could not register schema", zap.Error(err))
		return nil, 0, errors.Trace(err)
	}

	cacheEntry := new(schemaCacheEntry)
	cacheEntry.codec = codec
	cacheEntry.registryID = id
	cacheEntry.tiSchemaID = tiSchemaID

	m.cacheRWLock.Lock()
	m.cache[key] = cacheEntry
	m.cacheRWLock.Unlock()

	log.Info("Avro schema GetCachedOrRegister successful with cache miss",
		zap.Uint64("tiSchemaID", cacheEntry.tiSchemaID),
		zap.Int("registryID", cacheEntry.registryID),
		zap.String("schema", cacheEntry.codec.Schema()))

	return codec, id, nil
}

// ClearRegistry clears the Registry subject for the given table. Should be idempotent.
// Exported for testing.
// NOT USED for now, reserved for future use.
func (m *schemaManager) ClearRegistry(ctx context.Context, topicName string) error {
	uri := m.registryURL + "/subjects/" + url.QueryEscape(
		m.topicNameToSchemaSubject(topicName),
	)
	req, err := http.NewRequestWithContext(ctx, "DELETE", uri, nil)
	if err != nil {
		log.Error("Could not construct request for clearRegistry", zap.Error(err))
		return cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add(
		"Accept",
		"application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, "+
			"application/json",
	)
	resp, err := httpRetry(ctx, m.credential, req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == 200 {
		log.Info("Clearing Registry successful")
		return nil
	}

	if resp.StatusCode == 404 {
		log.Info("Registry already cleaned")
		return nil
	}

	log.Error("Error when clearing Registry", zap.Int("status", resp.StatusCode))
	return cerror.ErrAvroSchemaAPIError.GenWithStack(
		"Error when clearing Registry, status = %d",
		resp.StatusCode,
	)
}

func httpRetry(
	ctx context.Context,
	credential *security.Credential,
	r *http.Request,
) (*http.Response, error) {
	var (
		err  error
		resp *http.Response
		data []byte
	)

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxInterval = time.Second * 30
	httpCli, err := httputil.NewClient(credential)

	if r.Body != nil {
		data, err = io.ReadAll(r.Body)
		_ = r.Body.Close()
	}

	if err != nil {
		log.Error("Failed to parse response", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	for {
		if data != nil {
			r.Body = io.NopCloser(bytes.NewReader(data))
		}
		resp, err = httpCli.Do(r)

		if err != nil {
			log.Warn("HTTP request failed", zap.String("msg", err.Error()))
			goto checkCtx
		}

		// retry 4xx codes like 409 & 422 has no meaning since it's non-recoverable
		if resp.StatusCode >= 200 && resp.StatusCode < 300 ||
			(resp.StatusCode >= 400 && resp.StatusCode < 500) {
			break
		}
		log.Warn("HTTP server returned with error", zap.Int("status", resp.StatusCode))
		_, _ = io.Copy(io.Discard, resp.Body)
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

// TopicNameStrategy, ksqlDB only supports this
func (m *schemaManager) topicNameToSchemaSubject(topicName string) string {
	return topicName + m.subjectSuffix
}

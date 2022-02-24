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
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

// AvroSchemaManager is used to register Avro Schemas to the Registry server,
// look up local cache according to the table's name, and fetch from the Registry
// in cache the local cache entry is missing.
type AvroSchemaManager struct {
	registryURL   string
	subjectSuffix string

	credential *security.Credential

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

// NewAvroSchemaManager creates a new AvroSchemaManager
func NewAvroSchemaManager(
	ctx context.Context, credential *security.Credential, registryURL string, subjectSuffix string,
) (*AvroSchemaManager, error) {
	registryURL = strings.TrimRight(registryURL, "/")
	// Test connectivity to the Schema Registry
	req, err := http.NewRequestWithContext(ctx, "GET", registryURL, nil)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	httpCli, err := httputil.NewClient(credential)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := httpCli.Do(req)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "Test connection to Schema Registry failed")
	}
	defer resp.Body.Close()

	text, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "Reading response from Schema Registry failed")
	}

	if string(text[:]) != "{}" {
		return nil, cerror.ErrAvroSchemaAPIError.GenWithStack("Unexpected response from Schema Registry")
	}

	log.Info("Successfully tested connectivity to Schema Registry", zap.String("registryURL", registryURL))

	return &AvroSchemaManager{
		registryURL:   registryURL,
		cache:         make(map[string]*schemaCacheEntry, 1),
		subjectSuffix: subjectSuffix,
		credential:    credential,
	}, nil
}

var regexRemoveSpaces = regexp.MustCompile(`\s`)

// Register the latest schema for a table to the Registry, by passing in a Codec
// Returns the Schema's ID and err
func (m *AvroSchemaManager) Register(ctx context.Context, tableName model.TableName, codec *goavro.Codec) (int, error) {
	// The Schema Registry expects the JSON to be without newline characters
	reqBody := registerRequest{
		Schema: regexRemoveSpaces.ReplaceAllString(codec.Schema(), ""),
		// Commented out for compatibility with Confluent 5.4.x
		// SchemaType: "AVRO",
	}
	payload, err := json.Marshal(&reqBody)
	if err != nil {
		return 0, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "Could not marshal request to the Registry")
	}
	uri := m.registryURL + "/subjects/" + url.QueryEscape(m.tableNameToSchemaSubject(tableName)) + "/versions"
	log.Debug("Registering schema", zap.String("uri", uri), zap.ByteString("payload", payload))

	req, err := http.NewRequestWithContext(ctx, "POST", uri, bytes.NewReader(payload))
	if err != nil {
		return 0, cerror.ErrAvroSchemaAPIError.GenWithStackByArgs()
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json")
	resp, err := httpRetry(ctx, m.credential, req, false)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.Annotate(err, "Failed to read response from Registry")
	}

	if resp.StatusCode != 200 {
		log.Warn("Failed to register schema to the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.String("uri", uri),
			zap.ByteString("requestBody", payload),
			zap.ByteString("responseBody", body))
		return 0, cerror.ErrAvroSchemaAPIError.GenWithStack("Failed to register schema to the Registry, HTTP error")
	}

	var jsonResp registerResponse
	err = json.Unmarshal(body, &jsonResp)

	if err != nil {
		return 0, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "Failed to parse result from Registry")
	}

	if jsonResp.ID == 0 {
		return 0, cerror.ErrAvroSchemaAPIError.GenWithStack("Illegal schema ID returned from Registry %d", jsonResp.ID)
	}

	log.Info("Registered schema successfully",
		zap.Int("id", jsonResp.ID),
		zap.String("uri", uri),
		zap.ByteString("body", body))

	return jsonResp.ID, nil
}

// Lookup the latest schema and the Registry designated ID for that schema.
// TiSchemaId is only used to trigger fetching from the Registry server.
// Calling this method with a tiSchemaID other than that used last time will invariably trigger a RESTful request to the Registry.
// Returns (codec, registry schema ID, error)
// NOT USED for now, reserved for future use.
func (m *AvroSchemaManager) Lookup(ctx context.Context, tableName model.TableName, tiSchemaID uint64) (*goavro.Codec, int, error) {
	key := m.tableNameToSchemaSubject(tableName)
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

	uri := m.registryURL + "/subjects/" + url.QueryEscape(m.tableNameToSchemaSubject(tableName)) + "/versions/latest"
	log.Debug("Querying for latest schema", zap.String("uri", uri))

	req, err := http.NewRequestWithContext(ctx, "GET", uri, nil)
	if err != nil {
		return nil, 0, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "Error constructing request for Registry lookup")
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")

	resp, err := httpRetry(ctx, m.credential, req, true)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "Failed to read response from Registry")
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		log.Warn("Failed to query schema from the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.String("uri", uri),

			zap.ByteString("responseBody", body))
		return nil, 0, cerror.ErrAvroSchemaAPIError.GenWithStack("Failed to query schema from the Registry, HTTP error")
	}

	if resp.StatusCode == 404 {
		log.Warn("Specified schema not found in Registry",
			zap.String("key", key),
			zap.Uint64("tiSchemaID", tiSchemaID))

		return nil, 0, cerror.ErrAvroSchemaAPIError.GenWithStackByArgs("Schema not found in Registry")
	}

	var jsonResp lookupResponse
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, 0, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "Failed to parse result from Registry")
	}

	cacheEntry := new(schemaCacheEntry)
	cacheEntry.codec, err = goavro.NewCodec(jsonResp.Schema)
	if err != nil {
		return nil, 0, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "Creating Avro codec failed")
	}
	cacheEntry.registryID = jsonResp.RegistryID
	cacheEntry.tiSchemaID = tiSchemaID

	m.cacheRWLock.Lock()
	m.cache[m.tableNameToSchemaSubject(tableName)] = cacheEntry
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
func (m *AvroSchemaManager) GetCachedOrRegister(ctx context.Context, tableName model.TableName, tiSchemaID uint64, schemaGen SchemaGenerator) (*goavro.Codec, int, error) {
	key := m.tableNameToSchemaSubject(tableName)
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
		return nil, 0, errors.Annotate(err, "GetCachedOrRegister: SchemaGen failed")
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, 0, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "GetCachedOrRegister: Could not make goavro codec")
	}

	id, err := m.Register(ctx, tableName, codec)
	if err != nil {
		return nil, 0, errors.Annotate(
			cerror.WrapError(cerror.ErrAvroSchemaAPIError, err), "GetCachedOrRegister: Could not register schema")
	}

	cacheEntry := new(schemaCacheEntry)
	cacheEntry.codec = codec
	cacheEntry.registryID = id
	cacheEntry.tiSchemaID = tiSchemaID

	m.cacheRWLock.Lock()
	m.cache[m.tableNameToSchemaSubject(tableName)] = cacheEntry
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
func (m *AvroSchemaManager) ClearRegistry(ctx context.Context, tableName model.TableName) error {
	uri := m.registryURL + "/subjects/" + url.QueryEscape(m.tableNameToSchemaSubject(tableName))
	req, err := http.NewRequestWithContext(ctx, "DELETE", uri, nil)
	if err != nil {
		log.Error("Could not construct request for clearRegistry", zap.String("uri", uri))
		return cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
	resp, err := httpRetry(ctx, m.credential, req, true)
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
	return cerror.ErrAvroSchemaAPIError.GenWithStack("Error when clearing Registry, status = %d", resp.StatusCode)
}

func httpRetry(ctx context.Context, credential *security.Credential, r *http.Request, allow404 bool) (*http.Response, error) {
	var (
		err  error
		resp *http.Response
		data []byte
	)

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxInterval = time.Second * 30
	httpCli, err := httputil.NewClient(credential)

	if r.Body != nil {
		data, err = ioutil.ReadAll(r.Body)
		_ = r.Body.Close()
	}

	if err != nil {
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	for {
		if data != nil {
			r.Body = ioutil.NopCloser(bytes.NewReader(data))
		}
		resp, err = httpCli.Do(r)

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
	return tableName.Schema + "_" + tableName.Table + m.subjectSuffix
}

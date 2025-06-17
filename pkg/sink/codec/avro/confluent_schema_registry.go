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
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
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
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// confluent avro wire format, the first byte is always 0
// https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
const magicByte = uint8(0)

// confluentSchemaManager is used to register Avro Schemas to the confluent Registry server,
// look up local cache according to the table's name, and fetch from the Registry
// in cache the local cache entry is missing.
type confluentSchemaManager struct {
	registryURL string

	credential *security.Credential // placeholder, currently always nil

	cacheRWLock  sync.RWMutex
	cache        map[string]*schemaCacheEntry
	registryType string
}

type registerRequest struct {
	Schema string `json:"schema"`
	// Commented out for compatibility with Confluent 5.4.x
	// SchemaType string `json:"schemaType"`
}

type registerResponse struct {
	SchemaID int `json:"id"`
}

type lookupResponse struct {
	Name     string `json:"name"`
	SchemaID int    `json:"id"`
	Schema   string `json:"schema"`
}

// NewConfluentSchemaManager create schema managers,
// and test connectivity to the schema registry
func NewConfluentSchemaManager(
	ctx context.Context,
	registryURL string,
	credential *security.Credential,
) (SchemaManager, error) {
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
		log.Error("Unexpected response from Schema Registry", zap.ByteString("response", text))
		return nil, cerror.ErrAvroSchemaAPIError.GenWithStack(
			"Unexpected response from Schema Registry",
		)
	}

	log.Info(
		"Successfully tested connectivity to Schema Registry",
		zap.String("registryURL", registryURL),
	)

	return &confluentSchemaManager{
		registryURL:  registryURL,
		cache:        make(map[string]*schemaCacheEntry, 1),
		registryType: common.SchemaRegistryTypeConfluent,
	}, nil
}

// Register a schema in schema registry, no cache
func (m *confluentSchemaManager) Register(
	ctx context.Context,
	schemaName string,
	schemaDefinition string,
) (schemaID, error) {
	// The Schema Registry expects the JSON to be without newline characters
	id := schemaID{}

	buffer := new(bytes.Buffer)
	err := json.Compact(buffer, []byte(schemaDefinition))
	if err != nil {
		log.Error("Could not compact schema", zap.Error(err))
		return id, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	reqBody := registerRequest{
		Schema: buffer.String(),
	}
	payload, err := json.Marshal(&reqBody)
	if err != nil {
		log.Error("Could not marshal request to the Registry", zap.Error(err))
		return id, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	uri := m.registryURL + "/subjects/" + url.QueryEscape(schemaName) + "/versions"
	log.Info("Registering schema", zap.String("uri", uri), zap.ByteString("payload", payload))

	req, err := http.NewRequestWithContext(ctx, "POST", uri, bytes.NewReader(payload))
	if err != nil {
		log.Error("Failed to NewRequestWithContext", zap.Error(err))
		return id, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add(
		"Accept",
		"application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, "+
			"application/json",
	)
	req.Header.Add("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := httpRetry(ctx, m.credential, req)
	if err != nil {
		return id, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("Failed to read response from Registry", zap.Error(err))
		return id, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
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
		return id, cerror.ErrAvroSchemaAPIError.GenWithStackByArgs()
	}

	var jsonResp registerResponse
	err = json.Unmarshal(body, &jsonResp)

	if err != nil {
		log.Error("Failed to parse result from Registry", zap.Error(err))
		return id, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	if jsonResp.SchemaID == 0 {
		return id, cerror.ErrAvroSchemaAPIError.GenWithStack(
			"Illegal schema ID returned from Registry %d",
			jsonResp.SchemaID,
		)
	}

	log.Info("Registered schema successfully",
		zap.Int("schemaID", jsonResp.SchemaID),
		zap.String("uri", uri),
		zap.ByteString("body", body))

	id.confluentSchemaID = jsonResp.SchemaID
	return id, nil
}

// Lookup the cached schema entry first, if not found, fetch from the Registry server.
func (m *confluentSchemaManager) Lookup(
	ctx context.Context,
	schemaName string,
	schemaID schemaID,
) (*goavro.Codec, error) {
	m.cacheRWLock.RLock()
	entry, exists := m.cache[schemaName]
	if exists && entry.schemaID.confluentSchemaID == schemaID.confluentSchemaID {
		log.Debug("Avro schema lookup cache hit",
			zap.String("key", schemaName),
			zap.Int("schemaID", entry.schemaID.confluentSchemaID))
		m.cacheRWLock.RUnlock()
		return entry.codec, nil
	}
	m.cacheRWLock.RUnlock()

	log.Info("Avro schema lookup cache miss",
		zap.String("key", schemaName),
		zap.Int("schemaID", schemaID.confluentSchemaID))

	uri := m.registryURL + "/schemas/ids/" + strconv.Itoa(schemaID.confluentSchemaID)
	log.Debug("Querying for latest schema", zap.String("uri", uri))

	req, err := http.NewRequestWithContext(ctx, "GET", uri, nil)
	if err != nil {
		log.Error("Error constructing request for Registry lookup", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add(
		"Accept",
		"application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, "+
			"application/json",
	)

	resp, err := httpRetry(ctx, m.credential, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("Failed to parse result from Registry", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		log.Error("Failed to query schema from the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.String("uri", uri),
			zap.ByteString("responseBody", body))
		return nil, cerror.ErrAvroSchemaAPIError.GenWithStack(
			"Failed to query schema from the Registry, HTTP error",
		)
	}

	if resp.StatusCode == 404 {
		log.Warn("Specified schema not found in Registry",
			zap.String("key", schemaName),
			zap.Int("schemaID", schemaID.confluentSchemaID))
		return nil, cerror.ErrAvroSchemaAPIError.GenWithStackByArgs(
			"Schema not found in Registry",
		)
	}

	var jsonResp lookupResponse
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		log.Error("Failed to parse result from Registry", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	cacheEntry := new(schemaCacheEntry)
	cacheEntry.codec, err = goavro.NewCodec(jsonResp.Schema)
	if err != nil {
		log.Error("Creating Avro codec failed", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}
	cacheEntry.schemaID.confluentSchemaID = schemaID.confluentSchemaID
	cacheEntry.header, err = m.getMsgHeader(schemaID.confluentSchemaID)
	if err != nil {
		return nil, err
	}

	m.cacheRWLock.Lock()
	m.cache[schemaName] = cacheEntry
	m.cacheRWLock.Unlock()

	log.Info("Avro schema lookup successful with cache miss",
		zap.Int("schemaID", cacheEntry.schemaID.confluentSchemaID),
		zap.String("schema", cacheEntry.codec.Schema()))

	return cacheEntry.codec, nil
}

// GetCachedOrRegister checks if the suitable Avro schema has been cached.
// If not, a new schema is generated, registered and cached.
// Re-registering an existing schema shall return the same id(and version), so even if the
// cache is out-of-sync with schema registry, we could reload it.
func (m *confluentSchemaManager) GetCachedOrRegister(
	ctx context.Context,
	schemaSubject string,
	tableVersion uint64,
	schemaGen SchemaGenerator,
) (*goavro.Codec, []byte, error) {
	m.cacheRWLock.RLock()
	if entry, exists := m.cache[schemaSubject]; exists && entry.tableVersion == tableVersion {
		log.Debug("Avro schema GetCachedOrRegister cache hit",
			zap.String("key", schemaSubject),
			zap.Uint64("tableVersion", tableVersion),
			zap.Int("schemaID", entry.schemaID.confluentSchemaID))
		m.cacheRWLock.RUnlock()
		return entry.codec, entry.header, nil
	}
	m.cacheRWLock.RUnlock()

	log.Info("Avro schema lookup cache miss",
		zap.String("key", schemaSubject),
		zap.Uint64("tableVersion", tableVersion))

	schema, err := schemaGen()
	if err != nil {
		return nil, nil, err
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Error("GetCachedOrRegister: Could not make goavro codec", zap.Error(err))
		return nil, nil, cerror.WrapError(cerror.ErrAvroSchemaAPIError, err)
	}

	id, err := m.Register(ctx, schemaSubject, schema)
	if err != nil {
		log.Error("GetCachedOrRegister: Could not register schema", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}

	cacheEntry := new(schemaCacheEntry)
	cacheEntry.codec = codec
	cacheEntry.schemaID = id
	cacheEntry.tableVersion = tableVersion
	header, err := m.getMsgHeader(cacheEntry.schemaID.confluentSchemaID)
	if err != nil {
		return nil, nil, err
	}
	cacheEntry.header = header

	m.cacheRWLock.Lock()
	m.cache[schemaSubject] = cacheEntry
	m.cacheRWLock.Unlock()

	log.Info("Avro schema GetCachedOrRegister successful with cache miss",
		zap.Uint64("tableVersion", cacheEntry.tableVersion),
		zap.Int("schemaID", cacheEntry.schemaID.confluentSchemaID),
		zap.String("schema", cacheEntry.codec.Schema()))

	return codec, cacheEntry.header, nil
}

// ClearRegistry clears the Registry subject for the given table. Should be idempotent.
// Exported for testing.
// NOT USED for now, reserved for future use.
func (m *confluentSchemaManager) ClearRegistry(ctx context.Context, schemaSubject string) error {
	uri := m.registryURL + "/subjects/" + url.QueryEscape(schemaSubject)
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

func (m *confluentSchemaManager) RegistryType() string {
	return m.registryType
}

// confluent avro wire format, confluent avro is not same as apache avro
// https://rmoff.net/2020/07/03/why-json-isnt-the-same-as-json-schema-in-kafka-connect-converters \
// -and-ksqldb-viewing-kafka-messages-bytes-as-hex/
func (m *confluentSchemaManager) getMsgHeader(schemaID int) ([]byte, error) {
	head := new(bytes.Buffer)
	err := head.WriteByte(magicByte)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
	}
	err = binary.Write(head, binary.BigEndian, int32(schemaID))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
	}
	return head.Bytes(), nil
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

func getConfluentSchemaIDFromHeader(header []byte) (uint32, error) {
	if len(header) < 5 {
		return 0, cerror.ErrDecodeFailed.GenWithStackByArgs("header too short")
	}
	return binary.BigEndian.Uint32(header[1:5]), nil
}

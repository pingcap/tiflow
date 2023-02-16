// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/canal"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sink/codec/csv"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/factory"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	sinkutil "github.com/pingcap/tiflow/cdc/sinkv2/util"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/quotes"
	psink "github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/spanz"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	upstreamURIStr   string
	upstreamURI      *url.URL
	downstreamURIStr string
	configFile       string
	logFile          string
	logLevel         string
	flushInterval    time.Duration
)

const (
	defaultChangefeedName    = "storage-consumer"
	defaultFlushWaitDuration = 200 * time.Millisecond
)

func init() {
	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "storage uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&configFile, "config", "", "changefeed configuration file")
	flag.StringVar(&logFile, "log-file", "", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log level")
	flag.DurationVar(&flushInterval, "flush-interval", 10*time.Second, "flush interval")
	flag.Parse()

	err := logutil.InitLogger(&logutil.Config{
		Level: logLevel,
		File:  logFile,
	})
	if err != nil {
		log.Error("init logger failed", zap.Error(err))
		os.Exit(1)
	}

	uri, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Error("invalid upstream-uri", zap.Error(err))
		os.Exit(1)
	}
	upstreamURI = uri
	scheme := strings.ToLower(upstreamURI.Scheme)
	if !psink.IsStorageScheme(scheme) {
		log.Error("invalid storage scheme, the scheme of upstream-uri must be file/s3/azblob/gcs")
		os.Exit(1)
	}

	if len(configFile) == 0 {
		log.Error("changefeed configuration file must be provided")
		os.Exit(1)
	}
}

type schemaPathKey struct {
	schema  string
	table   string
	version int64
}

func (s schemaPathKey) generagteSchemaFilePath() string {
	return fmt.Sprintf("%s/%s/%d/schema.json", s.schema, s.table, s.version)
}

func (s *schemaPathKey) parseSchemaFilePath(path string) error {
	str := `(\w+)\/(\w+)\/(\d+)\/schema.json`
	pathRE, err := regexp.Compile(str)
	if err != nil {
		return err
	}

	matches := pathRE.FindStringSubmatch(path)
	if len(matches) != 4 {
		return fmt.Errorf("cannot match schema path pattern for %s", path)
	}

	version, err := strconv.ParseUint(matches[3], 10, 64)
	if err != nil {
		return err
	}

	*s = schemaPathKey{
		schema:  matches[1],
		table:   matches[2],
		version: int64(version),
	}
	return nil
}

type dmlPathKey struct {
	schemaPathKey
	partitionNum int64
	date         string
}

func (d *dmlPathKey) generateDMLFilePath(idx uint64, extension string) string {
	var elems []string

	elems = append(elems, d.schema)
	elems = append(elems, d.table)
	elems = append(elems, fmt.Sprintf("%d", d.version))

	if d.partitionNum != 0 {
		elems = append(elems, fmt.Sprintf("%d", d.partitionNum))
	}
	if len(d.date) != 0 {
		elems = append(elems, d.date)
	}
	elems = append(elems, fmt.Sprintf("CDC%06d%s", idx, extension))

	return strings.Join(elems, "/")
}

// dml file path pattern is as follows:
// {schema}/{table}/{table-version-separator}/{partition-separator}/{date-separator}/CDC{num}.extension
// in this pattern, partition-separator and date-separator could be empty.
func (d *dmlPathKey) parseDMLFilePath(dateSeparator, path string) (uint64, error) {
	var partitionNum int64

	str := `(\w+)\/(\w+)\/(\d+)\/(\d+\/)*`
	switch dateSeparator {
	case config.DateSeparatorNone.String():
		str += `(\d{4})*`
	case config.DateSeparatorYear.String():
		str += `(\d{4})\/`
	case config.DateSeparatorMonth.String():
		str += `(\d{4}-\d{2})\/`
	case config.DateSeparatorDay.String():
		str += `(\d{4}-\d{2}-\d{2})\/`
	}
	str += `CDC(\d+).\w+`
	pathRE, err := regexp.Compile(str)
	if err != nil {
		return 0, err
	}

	matches := pathRE.FindStringSubmatch(path)
	if len(matches) != 7 {
		return 0, fmt.Errorf("cannot match dml path pattern for %s", path)
	}

	version, err := strconv.ParseInt(matches[3], 10, 64)
	if err != nil {
		return 0, err
	}

	if len(matches[4]) > 0 {
		partitionNum, err = strconv.ParseInt(matches[4], 10, 64)
		if err != nil {
			return 0, err
		}
	}
	fileIdx, err := strconv.ParseUint(strings.TrimLeft(matches[6], "0"), 10, 64)
	if err != nil {
		return 0, err
	}

	*d = dmlPathKey{
		schemaPathKey: schemaPathKey{
			schema:  matches[1],
			table:   matches[2],
			version: version,
		},
		partitionNum: partitionNum,
		date:         matches[5],
	}

	return fileIdx, nil
}

// fileIndexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type fileIndexRange struct {
	start uint64
	end   uint64
}

type consumer struct {
	sinkFactory     *factory.SinkFactory
	replicationCfg  *config.ReplicaConfig
	codecCfg        *common.Config
	externalStorage storage.ExternalStorage
	fileExtension   string
	// tableIdxMap maintains a map of <dmlPathKey, max file index>
	tableIdxMap map[dmlPathKey]uint64
	// tableTsMap maintains a map of <TableID, max commit ts>
	tableTsMap map[model.TableID]uint64
	// tableSinkMap maintains a map of <TableID, TableSink>
	tableSinkMap     map[model.TableID]tablesink.TableSink
	tableIDGenerator *fakeTableIDGenerator
	errCh            chan error
}

func newConsumer(ctx context.Context) (*consumer, error) {
	replicaConfig := config.GetDefaultReplicaConfig()
	err := util.StrictDecodeFile(configFile, "storage consumer", replicaConfig)
	if err != nil {
		log.Error("failed to decode config file", zap.Error(err))
		return nil, err
	}

	err = replicaConfig.ValidateAndAdjust(upstreamURI)
	if err != nil {
		log.Error("failed to validate replica config", zap.Error(err))
		return nil, err
	}

	switch replicaConfig.Sink.Protocol {
	case config.ProtocolCsv.String():
	case config.ProtocolCanalJSON.String():
	default:
		return nil, fmt.Errorf("data encoded in protocol %s is not supported yet",
			replicaConfig.Sink.Protocol)
	}

	protocol, err := config.ParseSinkProtocolFromString(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, err
	}

	codecConfig := common.NewConfig(protocol)
	err = codecConfig.Apply(upstreamURI, replicaConfig)
	if err != nil {
		return nil, err
	}

	extension := sinkutil.GetFileExtension(protocol)

	storage, err := putil.GetExternalStorageFromURI(ctx, upstreamURIStr)
	if err != nil {
		log.Error("failed to create external storage", zap.Error(err))
		return nil, err
	}

	errCh := make(chan error, 1)
	stdCtx := contextutil.PutChangefeedIDInCtx(ctx,
		model.DefaultChangeFeedID(defaultChangefeedName))
	sinkFactory, err := factory.New(
		stdCtx,
		downstreamURIStr,
		config.GetDefaultReplicaConfig(),
		errCh,
	)
	if err != nil {
		log.Error("failed to create sink", zap.Error(err))
		return nil, err
	}

	return &consumer{
		sinkFactory:     sinkFactory,
		replicationCfg:  replicaConfig,
		codecCfg:        codecConfig,
		externalStorage: storage,
		fileExtension:   extension,
		errCh:           errCh,
		tableIdxMap:     make(map[dmlPathKey]uint64),
		tableTsMap:      make(map[model.TableID]uint64),
		tableSinkMap:    make(map[model.TableID]tablesink.TableSink),
		tableIDGenerator: &fakeTableIDGenerator{
			tableIDs: make(map[string]int64),
		},
	}, nil
}

// map1 - map2
func (c *consumer) difference(map1, map2 map[dmlPathKey]uint64) map[dmlPathKey]fileIndexRange {
	resMap := make(map[dmlPathKey]fileIndexRange)
	for k, v := range map1 {
		if _, ok := map2[k]; !ok {
			resMap[k] = fileIndexRange{
				start: 1,
				end:   v,
			}
		} else if v > map2[k] {
			resMap[k] = fileIndexRange{
				start: map2[k] + 1,
				end:   v,
			}
		}
	}

	return resMap
}

// getNewFiles returns dml files in specific ranges.
func (c *consumer) getNewFiles(ctx context.Context) (map[dmlPathKey]fileIndexRange, error) {
	m := make(map[dmlPathKey]fileIndexRange)
	opt := &storage.WalkOption{SubDir: ""}

	origTableMap := make(map[dmlPathKey]uint64, len(c.tableIdxMap))
	for k, v := range c.tableIdxMap {
		origTableMap[k] = v
	}

	schemaSet := make(map[schemaPathKey]struct{})
	err := c.externalStorage.WalkDir(ctx, opt, func(path string, size int64) error {
		var dmlkey dmlPathKey
		var schemaKey schemaPathKey

		if strings.HasSuffix(path, "metadata") {
			return nil
		}

		if strings.HasSuffix(path, "schema.json") {
			err := schemaKey.parseSchemaFilePath(path)
			if err != nil {
				log.Error("failed to parse schema file path", zap.Error(err))
			} else {
				schemaSet[schemaKey] = struct{}{}
			}

			return nil
		}

		fileIdx, err := dmlkey.parseDMLFilePath(c.replicationCfg.Sink.DateSeparator, path)
		if err != nil {
			log.Error("failed to parse dml file path", zap.Error(err))
			// skip handling this file
			return nil
		}

		if _, ok := c.tableIdxMap[dmlkey]; !ok || fileIdx >= c.tableIdxMap[dmlkey] {
			c.tableIdxMap[dmlkey] = fileIdx
		}

		return nil
	})
	if err != nil {
		return m, err
	}

	// filter out those files whose "schema.json" file has not been generated yet.
	// because we strongly rely on this schema file to get correct table definition
	// and do message decoding.
	for key := range c.tableIdxMap {
		schemaKey := key.schemaPathKey
		// cannot find the scheme file, filter out the item.
		if _, ok := schemaSet[schemaKey]; !ok {
			delete(c.tableIdxMap, key)
		}
	}

	m = c.difference(c.tableIdxMap, origTableMap)
	return m, err
}

// emitDMLEvents decodes RowChangedEvents from file content and emit them.
func (c *consumer) emitDMLEvents(ctx context.Context, tableID int64, pathKey dmlPathKey, content []byte) error {
	var (
		events      []*model.RowChangedEvent
		tableDetail cloudstorage.TableDefinition
		decoder     codec.EventBatchDecoder
		err         error
	)

	schemaFilePath := pathKey.schemaPathKey.generagteSchemaFilePath()
	schemaContent, err := c.externalStorage.ReadFile(ctx, schemaFilePath)
	if err != nil {
		return errors.Trace(err)
	}

	err = json.Unmarshal(schemaContent, &tableDetail)
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo, err := tableDetail.ToTableInfo()
	if err != nil {
		return errors.Trace(err)
	}

	switch c.codecCfg.Protocol {
	case config.ProtocolCsv:
		decoder, err = csv.NewBatchDecoder(ctx, c.codecCfg, tableInfo, content)
		if err != nil {
			return errors.Trace(err)
		}
	case config.ProtocolCanalJSON:
		decoder = canal.NewBatchDecoder(content, false, c.codecCfg.Terminator)
	}

	cnt := 0
	for {
		tp, hasNext, err := decoder.HasNext()
		if err != nil {
			log.Error("failed to decode message", zap.Error(err))
			return err
		}
		if !hasNext {
			break
		}
		cnt++

		if tp == model.MessageTypeRow {
			row, err := decoder.NextRowChangedEvent()
			if err != nil {
				log.Error("failed to get next row changed event", zap.Error(err))
				return errors.Trace(err)
			}

			if _, ok := c.tableSinkMap[tableID]; !ok {
				c.tableSinkMap[tableID] = c.sinkFactory.CreateTableSinkForConsumer(
					model.DefaultChangeFeedID(defaultChangefeedName),
					spanz.TableIDToComparableSpan(tableID),
					prometheus.NewCounter(prometheus.CounterOpts{}))
			}

			if _, ok := c.tableTsMap[tableID]; !ok || row.CommitTs >= c.tableTsMap[tableID] {
				c.tableTsMap[tableID] = row.CommitTs
			} else {
				log.Warn("row changed event commit ts fallback, ignore",
					zap.Uint64("commitTs", row.CommitTs),
					zap.Uint64("tableMaxCommitTs", c.tableTsMap[tableID]),
					zap.Any("row", row),
				)
				continue
			}
			row.Table.TableID = tableID
			events = append(events, row)
		}
	}
	log.Info("decode success", zap.String("schema", pathKey.schema),
		zap.String("table", pathKey.table),
		zap.Int64("version", pathKey.version),
		zap.Int("decodeRowsCnt", cnt),
		zap.Int("filteredRowsCnt", len(events)))

	c.tableSinkMap[tableID].AppendRowChangedEvents(events...)
	return err
}

func (c *consumer) waitTableFlushComplete(ctx context.Context, tableID model.TableID) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-c.errCh:
			return err
		default:
		}

		maxResolvedTs := c.tableTsMap[tableID]
		checkpoint := c.tableSinkMap[tableID].GetCheckpointTs()
		if checkpoint.Ts == maxResolvedTs {
			return nil
		}
		time.Sleep(defaultFlushWaitDuration)
	}
}

func (c *consumer) run(ctx context.Context) error {
	ticker := time.NewTicker(flushInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-c.errCh:
			return err
		case <-ticker.C:
		}

		fileMap, err := c.getNewFiles(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		keys := make([]dmlPathKey, 0, len(fileMap))
		for k := range fileMap {
			keys = append(keys, k)
		}

		if len(keys) == 0 {
			log.Info("no new files found since last round")
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].schema != keys[j].schema {
				return keys[i].schema < keys[j].schema
			}
			if keys[i].table != keys[j].table {
				return keys[i].table < keys[j].table
			}
			if keys[i].version != keys[j].version {
				return keys[i].version < keys[j].version
			}
			if keys[i].partitionNum != keys[j].partitionNum {
				return keys[i].partitionNum < keys[j].partitionNum
			}
			return keys[i].date < keys[j].date
		})

		for _, k := range keys {
			fileRange := fileMap[k]
			for i := fileRange.start; i <= fileRange.end; i++ {
				filePath := k.generateDMLFilePath(i, c.fileExtension)
				log.Debug("read from dml file path", zap.String("path", filePath))
				content, err := c.externalStorage.ReadFile(ctx, filePath)
				if err != nil {
					return errors.Trace(err)
				}
				tableID := c.tableIDGenerator.generateFakeTableID(
					k.schema, k.table, k.partitionNum)
				err = c.emitDMLEvents(ctx, tableID, k, content)
				if err != nil {
					return errors.Trace(err)
				}

				resolvedTs := model.NewResolvedTs(c.tableTsMap[tableID])
				err = c.tableSinkMap[tableID].UpdateResolvedTs(resolvedTs)
				if err != nil {
					return errors.Trace(err)
				}
				err = c.waitTableFlushComplete(ctx, tableID)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

// copied from kafka-consumer
type fakeTableIDGenerator struct {
	tableIDs       map[string]int64
	currentTableID int64
	mu             sync.Mutex
}

func (g *fakeTableIDGenerator) generateFakeTableID(schema, table string, partition int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := quotes.QuoteSchema(schema, table)
	if partition != 0 {
		key = fmt.Sprintf("%s.`%d`", key, partition)
	}
	if tableID, ok := g.tableIDs[key]; ok {
		return tableID
	}
	g.currentTableID++
	g.tableIDs[key] = g.currentTableID
	return g.currentTableID
}

func main() {
	var consumer *consumer
	var err error

	go func() {
		server := &http.Server{
			Addr:              ":6060",
			ReadHeaderTimeout: 5 * time.Second,
		}

		if err := server.ListenAndServe(); err != nil {
			log.Fatal("http pprof", zap.Error(err))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	deferFunc := func() int {
		stop()
		if consumer != nil {
			consumer.sinkFactory.Close()
		}
		if err != nil && err != context.Canceled {
			return 1
		}
		return 0
	}

	consumer, err = newConsumer(ctx)
	if err != nil {
		log.Error("failed to create storage consumer", zap.Error(err))
		goto EXIT
	}

	if err = consumer.run(ctx); err != nil {
		log.Error("error occurred while running consumer", zap.Error(err))
	}

EXIT:
	os.Exit(deferFunc())
}

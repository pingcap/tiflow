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

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/auth"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	ddlsinkfactory "github.com/pingcap/tiflow/cdc/sink/ddlsink/factory"
	eventsinkfactory "github.com/pingcap/tiflow/cdc/sink/dmlsink/factory"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	sutil "github.com/pingcap/tiflow/cdc/sink/util"
	cmdUtil "github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/canal"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	tpulsar "github.com/pingcap/tiflow/pkg/sink/pulsar"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// ConsumerOption represents the options of the pulsar consumer
type ConsumerOption struct {
	address []string
	topic   string

	protocol            config.Protocol
	enableTiDBExtension bool

	// the replicaConfig of the changefeed which produce data to the kafka topic
	replicaConfig *config.ReplicaConfig

	logPath       string
	logLevel      string
	timezone      string
	ca, cert, key string

	oauth2PrivateKey string
	oauth2IssuerURL  string
	oauth2ClientID   string
	oauth2Scope      string
	oauth2Audience   string

	mtlsAuthTLSCertificatePath string
	mtlsAuthTLSPrivateKeyPath  string

	downstreamURI string
	partitionNum  int
}

func newConsumerOption() *ConsumerOption {
	return &ConsumerOption{
		protocol: config.ProtocolCanalJSON,
		// the default value of partitionNum is 1
		partitionNum: 1,
	}
}

// Adjust the consumer option by the upstream uri passed in parameters.
func (o *ConsumerOption) Adjust(upstreamURI *url.URL, configFile string) {
	o.topic = strings.TrimFunc(upstreamURI.Path, func(r rune) bool {
		return r == '/'
	})
	o.address = strings.Split(upstreamURI.Host, ",")

	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile != "" {
		err := cmdUtil.StrictDecodeFile(configFile, "pulsar consumer", replicaConfig)
		if err != nil {
			log.Panic("decode config file failed", zap.Error(err))
		}
	}
	o.replicaConfig = replicaConfig

	s := upstreamURI.Query().Get("protocol")
	if s != "" {
		protocol, err := config.ParseSinkProtocolFromString(s)
		if err != nil {
			log.Panic("invalid protocol", zap.Error(err), zap.String("protocol", s))
		}
		o.protocol = protocol
	}
	if !sutil.IsPulsarSupportedProtocols(o.protocol) {
		log.Panic("unsupported protocol, pulsar sink currently only support these protocols: [canal-json]",
			zap.String("protocol", s))
	}

	s = upstreamURI.Query().Get("enable-tidb-extension")
	if s != "" {
		enableTiDBExtension, err := strconv.ParseBool(s)
		if err != nil {
			log.Panic("invalid enable-tidb-extension of upstream-uri")
		}
		o.enableTiDBExtension = enableTiDBExtension
	}

	log.Info("consumer option adjusted",
		zap.String("configFile", configFile),
		zap.String("address", strings.Join(o.address, ",")),
		zap.String("topic", o.topic),
		zap.Any("protocol", o.protocol),
		zap.Bool("enableTiDBExtension", o.enableTiDBExtension))
}

var (
	upstreamURIStr string
	configFile     string
	consumerOption = newConsumerOption()
)

func main() {
	cmd := &cobra.Command{
		Use: "pulsar consumer",
		Run: run,
	}
	// Flags for the root command
	cmd.Flags().StringVar(&configFile, "config", "", "config file for changefeed")
	cmd.Flags().StringVar(&upstreamURIStr, "upstream-uri", "", "pulsar uri")
	cmd.Flags().StringVar(&consumerOption.downstreamURI, "downstream-uri", "", "downstream sink uri")
	cmd.Flags().StringVar(&consumerOption.timezone, "tz", "System", "Specify time zone of pulsar consumer")
	cmd.Flags().StringVar(&consumerOption.ca, "ca", "", "CA certificate path for pulsar SSL connection")
	cmd.Flags().StringVar(&consumerOption.cert, "cert", "", "Certificate path for pulsar SSL connection")
	cmd.Flags().StringVar(&consumerOption.key, "key", "", "Private key path for pulsar SSL connection")
	cmd.Flags().StringVar(&consumerOption.logPath, "log-file", "cdc_pulsar_consumer.log", "log file path")
	cmd.Flags().StringVar(&consumerOption.logLevel, "log-level", "info", "log file path")
	cmd.Flags().StringVar(&consumerOption.oauth2PrivateKey, "oauth2-private-key", "", "oauth2 private key path")
	cmd.Flags().StringVar(&consumerOption.oauth2IssuerURL, "oauth2-issuer-url", "", "oauth2 issuer url")
	cmd.Flags().StringVar(&consumerOption.oauth2ClientID, "oauth2-client-id", "", "oauth2 client id")
	cmd.Flags().StringVar(&consumerOption.oauth2Audience, "oauth2-scope", "", "oauth2 scope")
	cmd.Flags().StringVar(&consumerOption.oauth2Audience, "oauth2-audience", "", "oauth2 audience")
	cmd.Flags().StringVar(&consumerOption.mtlsAuthTLSCertificatePath, "auth-tls-certificate-path", "", "mtls certificate path")
	cmd.Flags().StringVar(&consumerOption.mtlsAuthTLSPrivateKeyPath, "auth-tls-private-key-path", "", "mtls private key path")
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
	}
}

func run(_ *cobra.Command, _ []string) {
	err := logutil.InitLogger(&logutil.Config{
		Level: consumerOption.logLevel,
		File:  consumerOption.logPath,
	})
	if err != nil {
		log.Error("init logger failed", zap.Error(err))
		return
	}

	version.LogVersionInfo("pulsar consumer")

	upstreamURI, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Panic("invalid upstream-uri", zap.Error(err))
	}
	scheme := strings.ToLower(upstreamURI.Scheme)
	if !sink.IsPulsarScheme(scheme) {
		log.Panic("invalid upstream-uri scheme, the scheme of upstream-uri must be pulsar schema",
			zap.String("schema", scheme),
			zap.String("upstreamURI", upstreamURIStr))
	}

	consumerOption.Adjust(upstreamURI, configFile)

	ctx, cancel := context.WithCancel(context.Background())
	consumer, err := NewConsumer(ctx, consumerOption)
	if err != nil {
		log.Panic("Error creating pulsar consumer", zap.Error(err))
	}

	pulsarConsumer, client := NewPulsarConsumer(consumerOption)
	defer client.Close()
	defer pulsarConsumer.Close()
	msgChan := pulsarConsumer.Chan()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Info("terminating: context cancelled")
				return
			case consumerMsg := <-msgChan:
				log.Debug(fmt.Sprintf("Received message msgId: %#v -- content: '%s'\n",
					consumerMsg.ID(),
					string(consumerMsg.Payload())))
				err = consumer.HandleMsg(consumerMsg.Message)
				if err != nil {
					log.Panic("Error consuming message", zap.Error(err))
				}
				err = pulsarConsumer.AckID(consumerMsg.Message.ID())
				if err != nil {
					log.Panic("Error ack message", zap.Error(err))
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = consumer.Run(ctx); err != nil {
			if err != context.Canceled {
				log.Panic("Error running consumer", zap.Error(err))
			}
		}
	}()

	log.Info("TiCDC consumer up and running!...")
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Info("terminating: context cancelled")
	case <-sigterm:
		log.Info("terminating: via signal")
	}
	cancel()
	wg.Wait()
}

// NewPulsarConsumer creates a pulsar consumer
func NewPulsarConsumer(option *ConsumerOption) (pulsar.Consumer, pulsar.Client) {
	var pulsarURL string
	if len(option.ca) != 0 {
		pulsarURL = "pulsar+ssl" + "://" + option.address[0]
	} else {
		pulsarURL = "pulsar" + "://" + option.address[0]
	}
	topicName := option.topic
	subscriptionName := "pulsar-test-subscription"

	clientOption := pulsar.ClientOptions{
		URL:    pulsarURL,
		Logger: tpulsar.NewPulsarLogger(log.L()),
	}
	if len(option.ca) != 0 {
		clientOption.TLSTrustCertsFilePath = option.ca
		clientOption.TLSCertificateFile = option.cert
		clientOption.TLSKeyFilePath = option.key
	}

	var authentication pulsar.Authentication
	if len(option.oauth2PrivateKey) != 0 {
		authentication = pulsar.NewAuthenticationOAuth2(map[string]string{
			auth.ConfigParamIssuerURL: option.oauth2IssuerURL,
			auth.ConfigParamAudience:  option.oauth2Audience,
			auth.ConfigParamKeyFile:   option.oauth2PrivateKey,
			auth.ConfigParamClientID:  option.oauth2ClientID,
			auth.ConfigParamScope:     option.oauth2Scope,
			auth.ConfigParamType:      auth.ConfigParamTypeClientCredentials,
		})
		log.Info("oauth2 authentication is enabled", zap.String("issuer url", option.oauth2IssuerURL))
		clientOption.Authentication = authentication
	}
	if len(option.mtlsAuthTLSCertificatePath) != 0 {
		authentication = pulsar.NewAuthenticationTLS(option.mtlsAuthTLSCertificatePath, option.mtlsAuthTLSPrivateKeyPath)
		log.Info("mtls authentication is enabled",
			zap.String("cert", option.mtlsAuthTLSCertificatePath),
			zap.String("key", option.mtlsAuthTLSPrivateKeyPath),
		)
		clientOption.Authentication = authentication
	}

	client, err := pulsar.NewClient(clientOption)
	if err != nil {
		log.Fatal("can't create pulsar client", zap.Error(err))
	}

	consumerConfig := pulsar.ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	}

	consumer, err := client.Subscribe(consumerConfig)
	if err != nil {
		log.Fatal("can't create pulsar consumer", zap.Error(err))
	}
	return consumer, client
}

// partitionSinks maintained for each partition, it may sync data for multiple tables.
type partitionSinks struct {
	decoder codec.RowEventDecoder

	tablesCommitTsMap sync.Map
	tableSinksMap     sync.Map
	// resolvedTs record the maximum timestamp of the received event
	resolvedTs uint64
}

// Consumer represents a local pulsar consumer
type Consumer struct {
	eventGroups     map[int64]*eventsGroup
	ddlList         []*model.DDLEvent
	ddlListMu       sync.Mutex
	lastReceivedDDL *model.DDLEvent
	ddlSink         ddlsink.Sink

	// sinkFactory is used to create table sink for each table.
	sinkFactory *eventsinkfactory.SinkFactory
	sinks       []*partitionSinks
	sinksMu     sync.Mutex

	// initialize to 0 by default
	globalResolvedTs uint64

	tz *time.Location

	codecConfig *common.Config

	option *ConsumerOption
}

// NewConsumer creates a new cdc pulsar consumer
// the consumer is responsible for consuming the data from the pulsar topic
// and write the data to the downstream.
func NewConsumer(ctx context.Context, o *ConsumerOption) (*Consumer, error) {
	c := new(Consumer)
	c.option = o

	tz, err := util.GetTimezone(o.timezone)
	if err != nil {
		return nil, errors.Annotate(err, "can not load timezone")
	}
	config.GetGlobalServerConfig().TZ = o.timezone
	c.tz = tz

	c.codecConfig = common.NewConfig(o.protocol)
	c.codecConfig.EnableTiDBExtension = o.enableTiDBExtension
	decoder, err := canal.NewBatchDecoder(ctx, c.codecConfig, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.sinks = make([]*partitionSinks, o.partitionNum)
	for i := 0; i < o.partitionNum; i++ {
		c.sinks[i] = &partitionSinks{
			decoder: decoder,
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	errChan := make(chan error, 1)
	changefeedID := model.DefaultChangeFeedID("pulsar-consumer")
	f, err := eventsinkfactory.New(ctx, changefeedID, o.downstreamURI, o.replicaConfig, errChan, nil)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	c.sinkFactory = f

	go func() {
		err := <-errChan
		if errors.Cause(err) != context.Canceled {
			log.Error("error on running consumer", zap.Error(err))
		} else {
			log.Info("consumer exited")
		}
		cancel()
	}()

	ddlSink, err := ddlsinkfactory.New(ctx, changefeedID, o.downstreamURI, o.replicaConfig)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	c.ddlSink = ddlSink
	c.eventGroups = make(map[int64]*eventsGroup)
	return c, nil
}

type eventsGroup struct {
	events []*model.RowChangedEvent
}

func newEventsGroup() *eventsGroup {
	return &eventsGroup{
		events: make([]*model.RowChangedEvent, 0),
	}
}

func (g *eventsGroup) Append(e *model.RowChangedEvent) {
	g.events = append(g.events, e)
}

func (g *eventsGroup) Resolve(resolveTs uint64) []*model.RowChangedEvent {
	sort.Slice(g.events, func(i, j int) bool {
		return g.events[i].CommitTs < g.events[j].CommitTs
	})

	i := sort.Search(len(g.events), func(i int) bool {
		return g.events[i].CommitTs > resolveTs
	})
	result := g.events[:i]
	g.events = g.events[i:]

	return result
}

// HandleMsg handles the message received from the pulsar consumer
func (c *Consumer) HandleMsg(msg pulsar.Message) error {
	c.sinksMu.Lock()
	sink := c.sinks[0]
	c.sinksMu.Unlock()

	decoder := sink.decoder
	if err := decoder.AddKeyValue([]byte(msg.Key()), msg.Payload()); err != nil {
		log.Error("add key value to the decoder failed", zap.Error(err))
		return errors.Trace(err)
	}

	counter := 0
	for {
		tp, hasNext, err := decoder.HasNext()
		if err != nil {
			log.Panic("decode message key failed", zap.Error(err))
		}
		if !hasNext {
			break
		}

		counter++
		switch tp {
		case model.MessageTypeDDL:
			// for some protocol, DDL would be dispatched to all partitions,
			// Consider that DDL a, b, c received from partition-0, the latest DDL is c,
			// if we receive `a` from partition-1, which would be seemed as DDL regression,
			// then cause the consumer panic, but it was a duplicate one.
			// so we only handle DDL received from partition-0 should be enough.
			// but all DDL event messages should be consumed.
			ddl, err := decoder.NextDDLEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.ByteString("value", msg.Payload()),
					zap.Error(err))
			}
			c.appendDDL(ddl)
		case model.MessageTypeRow:
			row, err := decoder.NextRowChangedEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.ByteString("value", msg.Payload()),
					zap.Error(err))
			}
			globalResolvedTs := atomic.LoadUint64(&c.globalResolvedTs)
			partitionResolvedTs := atomic.LoadUint64(&sink.resolvedTs)
			if row.CommitTs <= globalResolvedTs || row.CommitTs <= partitionResolvedTs {
				log.Warn("RowChangedEvent fallback row, ignore it",
					zap.Uint64("commitTs", row.CommitTs),
					zap.Uint64("globalResolvedTs", globalResolvedTs),
					zap.Uint64("partitionResolvedTs", partitionResolvedTs),
					zap.Int32("partition", msg.ID().PartitionIdx()),
					zap.Any("row", row))
				// todo: mark the offset after the DDL is fully synced to the downstream mysql.
				continue
			}
			tableID := row.GetTableID()
			group, ok := c.eventGroups[tableID]
			if !ok {
				group = newEventsGroup()
				c.eventGroups[tableID] = group
			}
			group.Append(row)
			log.Info("DML event received",
				zap.Int64("tableID", row.GetTableID()),
				zap.String("schema", row.TableInfo.GetSchemaName()),
				zap.String("table", row.TableInfo.GetTableName()),
				zap.Uint64("commitTs", row.CommitTs),
				zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns))
		case model.MessageTypeResolved:
			ts, err := decoder.NextResolvedEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.ByteString("value", msg.Payload()),
					zap.Error(err))
			}

			globalResolvedTs := atomic.LoadUint64(&c.globalResolvedTs)
			partitionResolvedTs := atomic.LoadUint64(&sink.resolvedTs)
			if ts < globalResolvedTs || ts < partitionResolvedTs {
				log.Warn("partition resolved ts fallback, skip it",
					zap.Uint64("ts", ts),
					zap.Uint64("partitionResolvedTs", partitionResolvedTs),
					zap.Uint64("globalResolvedTs", globalResolvedTs),
					zap.Int32("partition", msg.ID().PartitionIdx()))
				continue
			}

			for tableID, group := range c.eventGroups {
				events := group.Resolve(ts)
				if len(events) == 0 {
					continue
				}
				if _, ok := sink.tableSinksMap.Load(tableID); !ok {
					log.Info("create table sink for consumer", zap.Any("tableID", tableID))
					tableSink := c.sinkFactory.CreateTableSinkForConsumer(
						model.DefaultChangeFeedID("pulsar-consumer"),
						spanz.TableIDToComparableSpan(tableID),
						events[0].CommitTs)

					log.Info("table sink created", zap.Any("tableID", tableID),
						zap.Any("tableSink", tableSink.GetCheckpointTs()))

					sink.tableSinksMap.Store(tableID, tableSink)
				}
				s, _ := sink.tableSinksMap.Load(tableID)
				s.(tablesink.TableSink).AppendRowChangedEvents(events...)
				commitTs := events[len(events)-1].CommitTs
				lastCommitTs, ok := sink.tablesCommitTsMap.Load(tableID)
				if !ok || lastCommitTs.(uint64) < commitTs {
					sink.tablesCommitTsMap.Store(tableID, commitTs)
				}
			}
			atomic.StoreUint64(&sink.resolvedTs, ts)
			log.Info("resolved ts updated", zap.Uint64("resolvedTs", ts))
		}

	}
	return nil
}

// append DDL wait to be handled, only consider the constraint among DDLs.
// for DDL a / b received in the order, a.CommitTs < b.CommitTs should be true.
func (c *Consumer) appendDDL(ddl *model.DDLEvent) {
	c.ddlListMu.Lock()
	defer c.ddlListMu.Unlock()
	// DDL CommitTs fallback, just crash it to indicate the bug.
	if c.lastReceivedDDL != nil && ddl.CommitTs < c.lastReceivedDDL.CommitTs {
		log.Panic("DDL CommitTs < lastReceivedDDL.CommitTs",
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.Uint64("lastReceivedDDLCommitTs", c.lastReceivedDDL.CommitTs),
			zap.Uint64("commitTs", ddl.CommitTs), zap.String("DDL", ddl.Query))
	}

	// A rename tables DDL job contains multiple DDL events with same CommitTs.
	// So to tell if a DDL is redundant or not, we must check the equivalence of
	// the current DDL and the DDL with max CommitTs.
	if ddl == c.lastReceivedDDL {
		log.Info("ignore redundant DDL, the DDL is equal to ddlWithMaxCommitTs",
			zap.Uint64("commitTs", ddl.CommitTs), zap.String("DDL", ddl.Query))
		return
	}

	c.ddlList = append(c.ddlList, ddl)
	log.Info("DDL event received", zap.Uint64("commitTs", ddl.CommitTs), zap.String("DDL", ddl.Query))
	c.lastReceivedDDL = ddl
}

func (c *Consumer) getFrontDDL() *model.DDLEvent {
	c.ddlListMu.Lock()
	defer c.ddlListMu.Unlock()
	if len(c.ddlList) > 0 {
		return c.ddlList[0]
	}
	return nil
}

func (c *Consumer) popDDL() *model.DDLEvent {
	c.ddlListMu.Lock()
	defer c.ddlListMu.Unlock()
	if len(c.ddlList) > 0 {
		ddl := c.ddlList[0]
		c.ddlList = c.ddlList[1:]
		return ddl
	}
	return nil
}

func (c *Consumer) forEachSink(fn func(sink *partitionSinks) error) error {
	c.sinksMu.Lock()
	defer c.sinksMu.Unlock()
	for _, sink := range c.sinks {
		if err := fn(sink); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// getMinResolvedTs returns the minimum resolvedTs of all the partitionSinks
func (c *Consumer) getMinResolvedTs() uint64 {
	result := uint64(math.MaxUint64)
	_ = c.forEachSink(func(sink *partitionSinks) error {
		a := atomic.LoadUint64(&sink.resolvedTs)
		if a < result {
			result = a
		}
		return nil
	})
	return result
}

// Run the Consumer
func (c *Consumer) Run(ctx context.Context) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			minResolvedTs := c.getMinResolvedTs()

			// 2. check if there is a DDL event that can be executed
			//   if there is, execute it and update the minResolvedTs
			nextDDL := c.getFrontDDL()
			if nextDDL != nil {
				log.Info("get nextDDL", zap.Any("DDL", nextDDL))
			}
			if nextDDL != nil && minResolvedTs >= nextDDL.CommitTs {
				// flush DMLs that commitTs <= todoDDL.CommitTs
				if err := c.forEachSink(func(sink *partitionSinks) error {
					return flushRowChangedEvents(ctx, sink, nextDDL.CommitTs)
				}); err != nil {
					return errors.Trace(err)
				}
				log.Info("begin to execute DDL", zap.Any("DDL", nextDDL))
				// all DMLs with commitTs <= todoDDL.CommitTs have been flushed to downstream,
				// so we can execute the DDL now.
				if err := c.ddlSink.WriteDDLEvent(ctx, nextDDL); err != nil {
					return errors.Trace(err)
				}
				ddl := c.popDDL()
				log.Info("DDL executed", zap.Any("DDL", ddl))
				minResolvedTs = ddl.CommitTs
			}

			// 3. Update global resolved ts
			if c.globalResolvedTs > minResolvedTs {
				log.Panic("global ResolvedTs fallback",
					zap.Uint64("globalResolvedTs", c.globalResolvedTs),
					zap.Uint64("minPartitionResolvedTs", minResolvedTs))
			}

			if c.globalResolvedTs < minResolvedTs {
				c.globalResolvedTs = minResolvedTs
			}

			// 4. flush all the DMLs that commitTs <= globalResolvedTs
			if err := c.forEachSink(func(sink *partitionSinks) error {
				return flushRowChangedEvents(ctx, sink, c.globalResolvedTs)
			}); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// flushRowChangedEvents flushes all the DMLs that commitTs <= resolvedTs
// Note: This function is synchronous, it will block until all the DMLs are flushed.
func flushRowChangedEvents(ctx context.Context, sink *partitionSinks, resolvedTs uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		flushedResolvedTs := true
		sink.tablesCommitTsMap.Range(func(key, value interface{}) bool {
			tableID := key.(int64)
			resolvedTs := model.NewResolvedTs(resolvedTs)
			tableSink, ok := sink.tableSinksMap.Load(tableID)
			if !ok {
				log.Panic("Table sink not found", zap.Int64("tableID", tableID))
			}
			if err := tableSink.(tablesink.TableSink).UpdateResolvedTs(resolvedTs); err != nil {
				log.Error("Failed to update resolved ts", zap.Error(err))
				return false
			}
			if !tableSink.(tablesink.TableSink).GetCheckpointTs().EqualOrGreater(resolvedTs) {
				flushedResolvedTs = false
			}
			return true
		})
		if flushedResolvedTs {
			return nil
		}
	}
}

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

package cmd

import (
	"context"
	"encoding/json"
	liberrors "errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/http/httpproxy"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
)

var (
	caPath        string
	certPath      string
	keyPath       string
	allowedCertCN string
)

var errOwnerNotFound = liberrors.New("owner not found")

var tsGapWarnning int64 = 86400 * 1000 // 1 day in milliseconds

func addSecurityFlags(flags *pflag.FlagSet, isServer bool) {
	flags.StringVar(&caPath, "ca", "", "CA certificate path for TLS connection")
	flags.StringVar(&certPath, "cert", "", "Certificate path for TLS connection")
	flags.StringVar(&keyPath, "key", "", "Private key path for TLS connection")
	if isServer {
		flags.StringVar(&allowedCertCN, "cert-allowed-cn", "", "Verify caller's identity (cert Common Name). Use ',' to separate multiple CN")
	}
}

func getCredential() *security.Credential {
	var certAllowedCN []string
	if len(allowedCertCN) != 0 {
		certAllowedCN = strings.Split(allowedCertCN, ",")
	}
	return &security.Credential{
		CAPath:        caPath,
		CertPath:      certPath,
		KeyPath:       keyPath,
		CertAllowedCN: certAllowedCN,
	}
}

// initCmd initializes the logger, the default context and returns its cancel function.
func initCmd(cmd *cobra.Command, logCfg *logutil.Config) context.CancelFunc {
	// Init log.
	err := logutil.InitLogger(logCfg)
	if err != nil {
		cmd.Printf("init logger error %v\n", errors.ErrorStack(err))
		os.Exit(1)
	}
	log.Info("init log", zap.String("file", logCfg.File), zap.String("level", logCfg.Level))

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := <-sc
		log.Info("got signal to exit", zap.Stringer("signal", sig))
		cancel()
	}()
	defaultContext = ctx
	return cancel
}

func getAllCaptures(ctx context.Context) ([]*capture, error) {
	_, raw, err := cdcEtcdCli.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}
	ownerID, err := cdcEtcdCli.GetOwnerID(ctx, kv.CaptureOwnerKey)
	if err != nil && errors.Cause(err) != concurrency.ErrElectionNoLeader {
		return nil, err
	}
	captures := make([]*capture, 0, len(raw))
	for _, c := range raw {
		isOwner := c.ID == ownerID
		captures = append(captures,
			&capture{ID: c.ID, IsOwner: isOwner, AdvertiseAddr: c.AdvertiseAddr})
	}
	return captures, nil
}

func getOwnerCapture(ctx context.Context) (*capture, error) {
	captures, err := getAllCaptures(ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range captures {
		if c.IsOwner {
			return c, nil
		}
	}
	return nil, errors.Trace(errOwnerNotFound)
}

func applyAdminChangefeed(ctx context.Context, job model.AdminJob, credential *security.Credential) error {
	owner, err := getOwnerCapture(ctx)
	if err != nil {
		return err
	}
	scheme := "http"
	if credential.IsTLSEnabled() {
		scheme = "https"
	}
	addr := fmt.Sprintf("%s://%s/capture/owner/admin", scheme, owner.AdvertiseAddr)
	cli, err := httputil.NewClient(credential)
	if err != nil {
		return err
	}
	forceRemoveOpt := "false"
	if job.Opts != nil && job.Opts.ForceRemove {
		forceRemoveOpt = "true"
	}
	resp, err := cli.PostForm(addr, url.Values(map[string][]string{
		cdc.APIOpVarAdminJob:           {fmt.Sprint(int(job.Type))},
		cdc.APIOpVarChangefeedID:       {job.CfID},
		cdc.APIOpForceRemoveChangefeed: {forceRemoveOpt},
	}))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.BadRequestf("admin changefeed failed")
		}
		return errors.BadRequestf("%s", string(body))
	}
	return nil
}

func applyOwnerChangefeedQuery(
	ctx context.Context, cid model.ChangeFeedID, credential *security.Credential,
) (string, error) {
	owner, err := getOwnerCapture(ctx)
	if err != nil {
		return "", err
	}
	scheme := "http"
	if credential.IsTLSEnabled() {
		scheme = "https"
	}
	addr := fmt.Sprintf("%s://%s/capture/owner/changefeed/query", scheme, owner.AdvertiseAddr)
	cli, err := httputil.NewClient(credential)
	if err != nil {
		return "", err
	}
	resp, err := cli.PostForm(addr, url.Values(map[string][]string{
		cdc.APIOpVarChangefeedID: {cid},
	}))
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.BadRequestf("query changefeed simplified status")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", errors.BadRequestf("%s", string(body))
	}
	return string(body), nil
}

func jsonPrint(cmd *cobra.Command, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	cmd.Printf("%s\n", data)
	return nil
}

func verifyStartTs(ctx context.Context, changefeedID string, startTs uint64) error {
	if disableGCSafePointCheck {
		return nil
	}
	return util.CheckSafetyOfStartTs(ctx, pdCli, changefeedID, startTs)
}

func verifyTargetTs(ctx context.Context, startTs, targetTs uint64) error {
	if targetTs > 0 && targetTs <= startTs {
		return errors.Errorf("target-ts %d must be larger than start-ts: %d", targetTs, startTs)
	}
	return nil
}

func verifyTables(ctx context.Context, credential *security.Credential, cfg *config.ReplicaConfig, startTs uint64) (ineligibleTables, eligibleTables []model.TableName, err error) {
	kvStore, err := kv.CreateTiStore(cliPdAddr, credential)
	if err != nil {
		return nil, nil, err
	}
	meta, err := kv.GetSnapshotMeta(kvStore, startTs)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	filter, err := filter.NewFilter(cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	snap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, startTs, false /* explicitTables */)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for tID, tableName := range snap.CloneTables() {
		tableInfo, exist := snap.TableByID(int64(tID))
		if !exist {
			return nil, nil, errors.NotFoundf("table %d", int64(tID))
		}
		if filter.ShouldIgnoreTable(tableName.Schema, tableName.Table) {
			continue
		}
		if !tableInfo.IsEligible(false /* forceReplicate */) {
			ineligibleTables = append(ineligibleTables, tableName)
		} else {
			eligibleTables = append(eligibleTables, tableName)
		}
	}
	return
}

func verifySink(
	ctx context.Context, sinkURI string, cfg *config.ReplicaConfig, opts map[string]string,
) error {
	filter, err := filter.NewFilter(cfg)
	if err != nil {
		return err
	}
	errCh := make(chan error)
	s, err := sink.NewSink(ctx, "cli-verify", sinkURI, filter, cfg, opts, errCh)
	if err != nil {
		return err
	}
	err = s.Close()
	if err != nil {
		return err
	}
	select {
	case err = <-errCh:
		if err != nil {
			return err
		}
	default:
	}
	return nil
}

// verifyReplicaConfig do strictDecodeFile check and only verify the rules for now
func verifyReplicaConfig(path, component string, cfg *config.ReplicaConfig) error {
	err := strictDecodeFile(path, component, cfg)
	if err != nil {
		return err
	}
	_, err = filter.VerifyRules(cfg)
	return err
}

// strictDecodeFile decodes the toml file strictly. If any item in confFile file is not mapped
// into the Config struct, issue an error and stop the server from starting.
func strictDecodeFile(path, component string, cfg interface{}) error {
	metaData, err := toml.DecodeFile(path, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	if undecoded := metaData.Undecoded(); len(undecoded) > 0 {
		var b strings.Builder
		for i, item := range undecoded {
			if i != 0 {
				b.WriteString(", ")
			}
			b.WriteString(item.String())
		}
		err = errors.Errorf("component %s's config file %s contained unknown configuration options: %s",
			component, path, b.String())
	}

	return errors.Trace(err)
}

// logHTTPProxies logs HTTP proxy relative environment variables.
func logHTTPProxies() {
	fields := proxyFields()
	if len(fields) > 0 {
		log.Info("using proxy config", fields...)
	}
}

func proxyFields() []zap.Field {
	proxyCfg := httpproxy.FromEnvironment()
	fields := make([]zap.Field, 0, 3)
	if proxyCfg.HTTPProxy != "" {
		fields = append(fields, zap.String("http_proxy", proxyCfg.HTTPProxy))
	}
	if proxyCfg.HTTPSProxy != "" {
		fields = append(fields, zap.String("https_proxy", proxyCfg.HTTPSProxy))
	}
	if proxyCfg.NoProxy != "" {
		fields = append(fields, zap.String("no_proxy", proxyCfg.NoProxy))
	}
	return fields
}

func confirmLargeDataGap(ctx context.Context, cmd *cobra.Command, startTs uint64) error {
	if noConfirm {
		return nil
	}
	currentPhysical, _, err := pdCli.GetTS(ctx)
	if err != nil {
		return err
	}
	tsGap := currentPhysical - oracle.ExtractPhysical(startTs)
	if tsGap > tsGapWarnning {
		cmd.Printf("Replicate lag (%s) is larger than 1 days, "+
			"large data may cause OOM, confirm to continue at your own risk [Y/N]\n",
			time.Duration(tsGap)*time.Millisecond,
		)
		var yOrN string
		_, err := fmt.Scan(&yOrN)
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
			return errors.NewNoStackError("abort changefeed create or resume")
		}
	}
	return nil
}

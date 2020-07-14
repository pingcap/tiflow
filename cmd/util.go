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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

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
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
)

var (
	caPath   string
	certPath string
	keyPath  string
)

func addSecurityFlags(flags *pflag.FlagSet) {
	flags.StringVar(&caPath, "ca", "", "CA certificate path for TLS connection")
	flags.StringVar(&certPath, "cert", "", "Certificate path for TLS connection")
	flags.StringVar(&keyPath, "key", "", "Private key path for TLS connection")
}

func getCredential() *security.Credential {
	return &security.Credential{
		CAPath:   caPath,
		CertPath: certPath,
		KeyPath:  keyPath,
	}
}

// initCmd initializes the logger, the default context and returns its cancel function.
func initCmd(cmd *cobra.Command, logCfg *util.Config) context.CancelFunc {
	// Init log.
	err := util.InitLogger(logCfg)
	if err != nil {
		cmd.Printf("init logger error %v\n", errors.ErrorStack(err))
		os.Exit(1)
	}
	log.Info("init log", zap.String("file", logFile), zap.String("level", logLevel))

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
	return nil, errors.NotFoundf("owner")
}

func applyAdminChangefeed(ctx context.Context, job model.AdminJob) error {
	owner, err := getOwnerCapture(ctx)
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("http://%s/capture/owner/admin", owner.AdvertiseAddr)
	resp, err := http.PostForm(addr, url.Values(map[string][]string{
		cdc.APIOpVarAdminJob:     {fmt.Sprint(int(job.Type))},
		cdc.APIOpVarChangefeedID: {job.CfID},
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

func applyOwnerChangefeedQuery(ctx context.Context, cid model.ChangeFeedID) (string, error) {
	owner, err := getOwnerCapture(ctx)
	if err != nil {
		return "", err
	}
	addr := fmt.Sprintf("http://%s/capture/owner/changefeed/query", owner.AdvertiseAddr)
	resp, err := http.PostForm(addr, url.Values(map[string][]string{
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

func verifyStartTs(ctx context.Context, startTs uint64, cli kv.CDCEtcdClient) error {
	resp, err := cli.Client.Get(ctx, tikv.GcSavedSafePoint)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Count == 0 {
		return nil
	}
	safePoint, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return errors.Trace(err)
	}
	if startTs < safePoint {
		return errors.Errorf("startTs %d less than gcSafePoint %d", startTs, safePoint)
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

	snap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, startTs)
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
		if !tableInfo.IsEligible() {
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
	s, err := sink.NewSink(ctx, sinkURI, filter, cfg, opts, errCh)
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

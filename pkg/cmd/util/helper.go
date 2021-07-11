package util

import (
	"context"
	"encoding/json"
	liberrors "errors"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/cli/capture"
	"github.com/pingcap/ticdc/pkg/cmd/cli/changefeed"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/tikv/client-go/v2/oracle"
	"go.etcd.io/etcd/clientv3/concurrency"
	"io/ioutil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cmdconetxt "github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/net/http/httpproxy"
)

// Endpoint schemes.
const (
	HTTP  = "http"
	HTTPS = "https"
)

var ErrOwnerNotFound = liberrors.New("owner not found")

var tsGapWarning int64 = 86400 * 1000 // 1 day in milliseconds

// InitCmd initializes the logger, the default context and returns its cancel function.
func InitCmd(cmd *cobra.Command, logCfg *logutil.Config) context.CancelFunc {
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
	cmdconetxt.SetDefaultContext(ctx)
	return cancel
}

// VerifyPdEndpoint verifies whether the pd endpoint is a valid http or https URL.
// The certificate is required when using https.
func VerifyPdEndpoint(pdEndpoint string, useTLS bool) error {
	u, err := url.Parse(pdEndpoint)
	if err != nil {
		return errors.Annotate(err, "parse PD endpoint")
	}
	if (u.Scheme != HTTP && u.Scheme != HTTPS) || u.Host == "" {
		return errors.New("PD endpoint should be a valid http or https URL")
	}

	if useTLS {
		if u.Scheme == HTTP {
			return errors.New("PD endpoint scheme should be https")
		}
	} else {
		if u.Scheme == HTTPS {
			return errors.New("PD endpoint scheme is https, please provide certificate")
		}
	}
	return nil
}

// LogHTTPProxies logs HTTP proxy relative environment variables.
func LogHTTPProxies() {
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

func JsonPrint(cmd *cobra.Command, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	cmd.Printf("%s\n", data)
	return nil
}

func getAllCaptures(f Factory, ctx context.Context) ([]*capture.Capture, error) {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return nil, err
	}
	_, raw, err := etcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}
	ownerID, err := etcdClient.GetOwnerID(ctx, kv.CaptureOwnerKey)
	if err != nil && errors.Cause(err) != concurrency.ErrElectionNoLeader {
		return nil, err
	}
	captures := make([]*capture.Capture, 0, len(raw))
	for _, c := range raw {
		isOwner := c.ID == ownerID
		captures = append(captures,
			&capture.Capture{ID: c.ID, IsOwner: isOwner, AdvertiseAddr: c.AdvertiseAddr})
	}
	return captures, nil
}

func getOwnerCapture(f Factory, ctx context.Context) (*capture.Capture, error) {
	captures, err := getAllCaptures(f, ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range captures {
		if c.IsOwner {
			return c, nil
		}
	}
	return nil, errors.Trace(ErrOwnerNotFound)
}

func ApplyOwnerChangefeedQuery(f Factory,
	ctx context.Context, cid model.ChangeFeedID, credential *security.Credential,
) (string, error) {
	owner, err := getOwnerCapture(f, ctx)
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

func ApplyAdminChangefeed(f Factory, ctx context.Context, job model.AdminJob, credential *security.Credential) error {
	owner, err := getOwnerCapture(f, ctx)
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

func ConfirmLargeDataGap(f Factory, ctx context.Context, cmd *cobra.Command, commonOptions *changefeed.CommonOptions, startTs uint64) error {
	if commonOptions.NoConfirm {
		return nil
	}
	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}
	currentPhysical, _, err := pdClient.GetTS(ctx)
	if err != nil {
		return err
	}
	tsGap := currentPhysical - oracle.ExtractPhysical(startTs)
	if tsGap > tsGapWarning {
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

// StrictDecodeFile decodes the toml file strictly. If any item in confFile file is not mapped
// into the Config struct, issue an error and stop the server from starting.
func StrictDecodeFile(path, component string, cfg interface{}) error {
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

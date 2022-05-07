package etcdutils

import (
	"fmt"
	"net/url"
	"time"

	"github.com/phayes/freeport"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/server/v3/embed"
)

// SetupEmbedEtcd starts an embed etcd server
// TODO make cluster size configurable
func SetupEmbedEtcd(dir string) (clientURLs []*url.URL, e *embed.Etcd, err error) {
	cfg := embed.NewConfig()
	cfg.Dir = dir

	ports, err := freeport.GetFreePorts(2)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	url0, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", ports[0]))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	url1, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", ports[1]))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	cfg.LPUrls = []url.URL{*url0}
	cfg.LCUrls = []url.URL{*url1}
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	clientURLs = []*url.URL{url0, url1}

	e, err = embed.StartEtcd(cfg)
	if err != nil {
		return
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		err = errors.New("server took too long to start")
	}

	return
}

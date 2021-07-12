package test

import (
	"fmt"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"os"
	"strings"
)

type testingT struct {
}

// Errorf implements require.TestingT
func (t *testingT) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

// FailNow implements require.TestingT
func (t *testingT) FailNow() {
	os.Exit(-1)
}

type Options struct {
	testPdAddr    string
	caPath        string
	certPath      string
	keyPath       string
	allowedCertCN string
}

func (o *Options) getCredential() *security.Credential {
	var certAllowedCN []string
	if len(o.allowedCertCN) != 0 {
		certAllowedCN = strings.Split(o.allowedCertCN, ",")
	}
	return &security.Credential{
		CAPath:        o.caPath,
		CertPath:      o.certPath,
		KeyPath:       o.keyPath,
		CertAllowedCN: certAllowedCN,
	}
}

func NewOptions() *Options {
	return &Options{}
}

func NewCmdTest() *cobra.Command {
	o := NewOptions()

	command := &cobra.Command{
		Hidden: true,
		Use:    "testkv",
		Short:  "test kv",
		Long:   ``,
		Run: func(cmd *cobra.Command, args []string) {
			addrs := strings.Split(o.testPdAddr, ",")
			cli, err := pd.NewClient(addrs, o.getCredential().PDSecurityOption())
			if err != nil {
				fmt.Println(err)
				return
			}

			storage, err := kv.CreateStorage(addrs[0])
			if err != nil {
				fmt.Println(err)
				return
			}

			tikvStorage := storage.(tikv.Storage) // we know it is tikv.

			t := new(testingT)
			kv.TestGetKVSimple(t, cli, tikvStorage, storage)
			kv.TestSplit(t, cli, tikvStorage, storage)
		},
	}

	command.Flags().StringVar(&o.testPdAddr, "pd", "http://127.0.0.1:2379", "address of PD")

	return command
}

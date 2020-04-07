package cmd

import (
	"fmt"
	"os"
	"strings"

	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/spf13/cobra"
)

var (
	testPdAddr string
)

func init() {
	rootCmd.AddCommand(testKVCmd)

	testKVCmd.Flags().StringVar(&testPdAddr, "pd", "http://127.0.0.1:2379", "address of PD")
}

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

var testKVCmd = &cobra.Command{
	Hidden: true,
	Use:    "testkv",
	Short:  "test kv",
	Long:   ``,
	Run: func(cmd *cobra.Command, args []string) {
		addrs := strings.Split(testPdAddr, ",")
		cli, err := pd.NewClient(addrs, pd.SecurityOption{})
		if err != nil {
			fmt.Println(err)
			return
		}

		storage, err := kv.CreateStorage(addrs[0])
		if err != nil {
			fmt.Println(err)
			return
		}

		t := new(testingT)
		kv.TestGetKVSimple(t, cli, storage)
		kv.TestSplit(t, cli, storage)
	},
}

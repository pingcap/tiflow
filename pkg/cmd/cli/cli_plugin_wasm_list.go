package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/errors"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

type listWasmPluginOptions struct {
	withBinary bool
}

func (u *listWasmPluginOptions) addFlags(cmd *cobra.Command) {
	if u == nil {
		return
	}

	cmd.PersistentFlags().BoolVar(&u.withBinary, "with-binary", false, "With wasm plugin binary")
}

func (u *listWasmPluginOptions) run(cmd *cobra.Command, f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	ctx := cmdcontext.GetDefaultContext()
	captures, err := listCaptures(ctx, etcdClient)
	if err != nil {
		return err
	}

	resps := make(map[string]string)
	for _, capture := range captures {
		resp, err := sendCaptureListWasmPluginQuery(ctx, capture, u.withBinary)
		if err != nil {
			return errors.WithMessage(err, fmt.Sprintf("upload failed, capture: %v", capture))
		}
		resps[capture.AdvertiseAddr] = resp
	}
	_ = util.JSONPrint(cmd, resps)
	return nil
}

func sendCaptureListWasmPluginQuery(ctx context.Context, capture *capture, withBinary bool) (string, error) {
	// TODO: https not supported
	url := fmt.Sprintf("http://%s/api/v1/plugins/wasm?with-binary=%v", capture.AdvertiseAddr, withBinary)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("status: %d, body: %s", resp.StatusCode, body), nil
	} else {
		return string(body), nil
	}
}

func newCmdPluginWasmList(f factory.Factory) *cobra.Command {
	o := &listWasmPluginOptions{}
	cmds := &cobra.Command{
		Use:   "plugin wasm list",
		Short: "Manage Wasm plugin upload",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(cmd, f)
		},
	}

	o.addFlags(cmds)

	return cmds
}

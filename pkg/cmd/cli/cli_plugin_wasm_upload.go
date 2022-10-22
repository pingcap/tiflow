package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/spf13/cobra"
)

type uploadWasmPluginOptions struct {
	name     string
	filepath string
}

func (u *uploadWasmPluginOptions) addFlags(cmd *cobra.Command) {
	if u == nil {
		return
	}

	cmd.PersistentFlags().StringVar(&u.filepath, "filepath", "", "Filepath of plugin")
	cmd.PersistentFlags().StringVar(&u.name, "name", "", "Name of plugin")
}

func (u *uploadWasmPluginOptions) run(f factory.Factory) error {
	if u.name == "" {
		return errors.New("null plugin name")
	}
	if u.filepath == "" {
		return errors.New("null plugin filepath")
	}
	binary, err := ioutil.ReadFile(u.filepath)
	if err != nil {
		return errors.WithMessage(err, "read plugin file error")
	}
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	ctx := cmdcontext.GetDefaultContext()
	captures, err := listCaptures(ctx, etcdClient)
	if err != nil {
		return err
	}

	for _, capture := range captures {
		_, err := sendCaptureUploadWasmPluginQuery(ctx, capture, u.name, binary)
		if err != nil {
			return errors.WithMessage(err, fmt.Sprintf("upload failed, capture: %v", capture))
		}
	}
	return nil
}

func sendCaptureUploadWasmPluginQuery(ctx context.Context, capture *capture, name string, binary []byte) (string, error) {
	// TODO: https not supported
	url := fmt.Sprintf("http://%s/api/v1/plugins/wasm/upload", capture.AdvertiseAddr)
	cfg := &model.WasmPluginConfig{
		Name:   name,
		Binary: string(binary),
	}
	body, _ := json.Marshal(cfg)
	buf := bytes.NewReader(body)
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Sprintf("status: %d, body: %s", resp.StatusCode, body), nil
	}
	return "OK", nil
}

func newCmdPluginWasmUpload(f factory.Factory) *cobra.Command {
	o := &uploadWasmPluginOptions{}
	cmds := &cobra.Command{
		Use:   "upload",
		Short: "Manage Wasm plugin upload",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(f)
		},
	}

	o.addFlags(cmds)

	return cmds
}

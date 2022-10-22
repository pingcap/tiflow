package api

import (
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/model"
)

const DefaultPluginPath = "/home/ubuntu/cdc_wasm_plugins"

func (h *openAPI) UploadWasmPlugin(c *gin.Context) {
	var cfg model.WasmPluginConfig
	if err := c.BindJSON(&cfg); err != nil {
		_ = c.Error(err)
		return
	}

	if err := saveWasmPlugin(cfg.Name, cfg.Binary); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusOK)
}

func (h *openAPI) ListWasmPlugins(c *gin.Context) {
	withBinary := c.Param("with-binary")
	binary, _ := strconv.ParseBool(withBinary)
	cfgs, err := listWasmPlugins(binary)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, cfgs)
}

func saveWasmPlugin(name string, binary string) error {
	fileName := getWasmPluginPath(name)
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	_, err = f.WriteString(binary)
	return err
}

func listWasmPlugins(withBinary bool) ([]*model.WasmPluginConfig, error) {
	infos, err := ioutil.ReadDir(DefaultPluginPath)
	if err != nil {
		return nil, err
	}
	var rets []*model.WasmPluginConfig
	for _, info := range infos {
		ret := &model.WasmPluginConfig{
			Name: info.Name(),
		}
		if withBinary {
			binary, err := ioutil.ReadFile(getWasmPluginPath(info.Name()))
			if err != nil {
				return nil, err
			}
			ret.Binary = string(binary)
		}
		rets = append(rets, ret)
	}
	return rets, nil
}

func getWasmPluginPath(name string) string {
	return path.Join(DefaultPluginPath, name)
}

package api

import (
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"

	"github.com/gin-gonic/gin"
)

const DefaultPluginPath = "/home/ubuntu/cdc_wasm_plugins"

type WasmPluginConfig struct {
	Name   string
	Binary string
}

func (h *openAPI) UploadWasmPlugin(c *gin.Context) {
	var cfg WasmPluginConfig
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

func listWasmPlugins(withBinary bool) ([]*WasmPluginConfig, error) {
	infos, err := ioutil.ReadDir(DefaultPluginPath)
	if err != nil {
		return nil, err
	}
	var rets []*WasmPluginConfig
	for _, info := range infos {
		ret := &WasmPluginConfig{
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

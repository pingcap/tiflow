package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/pingcap/dm/dm/portal"
	_ "github.com/pingcap/dm/dm/portal/statik"
	"github.com/pingcap/dm/pkg/log"
	"github.com/rakyll/statik/fs"
	"go.uber.org/zap"
)

func main() {
	cfg := portal.NewConfig()
	cfg.Parse(os.Args[1:])
	if err := cfg.Valid(); err != nil {
		log.L().Error("config is invalid, please check it", zap.Error(err))
		os.Exit(1)
	}
	fmt.Println(cfg)

	statikFS, err := fs.New()
	if err != nil {
		zap.L().Error("", zap.Error(err))
		os.Exit(1)
	}
	http.Handle("/", http.StripPrefix("/", http.FileServer(statikFS)))

	portal := portal.NewHandler(cfg.TaskFilePath, cfg.Timeout)

	http.HandleFunc("/check", portal.Check)
	http.HandleFunc("/schema", portal.GetSchemaInfo)
	http.HandleFunc("/generate_config", portal.GenerateConfig)
	http.HandleFunc("/analyze_config_file", portal.AnalyzeConfig)
	http.HandleFunc("/download", portal.Download)

	err = http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), nil)
	if err != nil {
		log.L().Error("listen and server failed", zap.Error(err))
		os.Exit(1)
	}
}

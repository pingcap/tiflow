// Copyright 2021 PingCAP, Inc.
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

package portal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	// for database.
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/unrolled/render"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	routeTp  = "route_rules"
	filterTp = "filter"

	baListTp = "ba_list"

	success = "success"
	failed  = "failed"
)

// Handler used for deal with http request.
type Handler struct {
	r *render.Render

	// the path used to save generated task config file
	path string

	// the timeout for connect database and query, unit: second
	timeout int
}

// NewHandler returns a new Handler.
func NewHandler(path string, timeout int) *Handler {
	rd := render.New(render.Options{
		IndentJSON: true,
	})

	return &Handler{
		path:    strings.TrimRight(path, "/"),
		timeout: timeout,
		r:       rd,
	}
}

// CommonResult is the common result.
type CommonResult struct {
	Result string `json:"result"`
	Error  string `json:"error"`
}

// CheckResult is the result of Check.
type CheckResult struct {
	CommonResult
}

// SchemaInfoResult is the result of GetSchemaInfo.
type SchemaInfoResult struct {
	CommonResult
	Tables []TablesInSchema `json:"tables"`
}

// TablesInSchema saves all the tables in one schema.
type TablesInSchema struct {
	Schema string   `json:"schema"`
	Tables []string `json:"tables"`
}

// GenerateConfigResult is the result of GenerateConfig.
type GenerateConfigResult struct {
	CommonResult
	Filepath string `json:"filepath"`
}

// AnalyzeResult is the result of AnalyzeConfig.
type AnalyzeResult struct {
	CommonResult
	Config DMTaskConfig `json:"config"`
}

// Check checks database can be connected.
func (p *Handler) Check(w http.ResponseWriter, req *http.Request) {
	log.L().Info("receive Check request")

	db, addr, err := getDBConnFunc(req, p.timeout)
	if err != nil {
		log.L().Error("connect to database failed", zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, CheckResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
		})
		return
	}
	defer db.Close()
	log.L().Info("check success", zap.String("address", addr))

	p.genJSONResp(w, http.StatusOK, CheckResult{
		CommonResult: CommonResult{
			Result: success,
			Error:  "",
		},
	})
}

// GetSchemaInfo gets all schemas and tables information from a database.
func (p *Handler) GetSchemaInfo(w http.ResponseWriter, req *http.Request) {
	log.L().Info("receive GetSchemaInfo request")

	db, addr, err := getDBConnFunc(req, p.timeout)
	if err != nil {
		p.genJSONResp(w, http.StatusBadRequest, SchemaInfoResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
		})
		return
	}
	defer db.Close()
	log.L().Info("find all the tables", zap.String("address", addr))

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.timeout)*time.Second)
	defer cancel()

	schemas, err := dbutil.GetSchemas(ctx, db)
	if err != nil {
		log.L().Error("get schemas failed", zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, SchemaInfoResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
		})
		return
	}

	allTables := make([]TablesInSchema, 0, len(schemas))
	for _, schema := range schemas {
		if filter.IsSystemSchema(schema) {
			continue
		}

		tables, err := dbutil.GetTables(ctx, db, schema)
		if err != nil {
			log.L().Error("get tables failed", zap.String("schema", schema), zap.Error(err))
			p.genJSONResp(w, http.StatusBadRequest, SchemaInfoResult{
				CommonResult: CommonResult{
					Result: failed,
					Error:  err.Error(),
				},
			})
			return
		}
		allTables = append(allTables, TablesInSchema{Schema: schema, Tables: tables})
	}

	p.genJSONResp(w, http.StatusOK, SchemaInfoResult{
		CommonResult: CommonResult{
			Result: success,
			Error:  "",
		},
		Tables: allTables,
	})
}

// GenerateConfig generates config file used for dm.
func (p *Handler) GenerateConfig(w http.ResponseWriter, req *http.Request) {
	log.L().Info("receive GenerateConfig request")

	taskCfg := &DMTaskConfig{}
	if err := readJSON(req.Body, taskCfg); err != nil {
		log.L().Error("read json failed", zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, GenerateConfigResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},

			Filepath: "",
		})
		return
	}
	if err := adjustConfig(taskCfg); err != nil {
		log.L().Error("adjust config failed", zap.Reflect("config", taskCfg), zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, GenerateConfigResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
			Filepath: "",
		})
		return
	}

	if err := taskCfg.Verify(); err != nil {
		log.L().Error("verify config failed", zap.Reflect("config", taskCfg), zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, GenerateConfigResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
			Filepath: "",
		})
		return
	}

	log.L().Info("generate config file", zap.String("config name", taskCfg.Name))

	filepath := path.Join(p.path, generateTaskFileName(taskCfg.Name))
	f, err := os.Create(filepath)
	if err != nil {
		log.L().Error("generate task file failed", zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, GenerateConfigResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
			Filepath: "",
		})
		return
	}
	defer f.Close()

	cfgBytes, err := yaml.Marshal(taskCfg)
	if err != nil {
		log.L().Error("marshal config failed", zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, GenerateConfigResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
			Filepath: "",
		})
		return
	}

	if _, err := f.Write(cfgBytes); err != nil {
		log.L().Error("write config data failed", zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, GenerateConfigResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
			Filepath: "",
		})
		return
	}

	p.genJSONResp(w, http.StatusOK, GenerateConfigResult{
		CommonResult: CommonResult{
			Result: success,
			Error:  "",
		},
		Filepath: filepath,
	})
}

// AnalyzeConfig analyzes dm task config from front-end.
func (p *Handler) AnalyzeConfig(w http.ResponseWriter, req *http.Request) {
	log.L().Info("receive AnalyzeConfig request")

	// Parse our multipart form, 10 << 20 specifies a maximum
	// upload of 10 MB files.
	//nolint:errcheck
	req.ParseMultipartForm(10 << 20)
	// FormFile returns the first file for the given key `taskfile`
	// it also returns the FileHeader so we can get the Filename,
	// the Header and the size of the file
	file, _, err := req.FormFile("taskfile")
	if err != nil {
		log.L().Error("form file failed", zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, AnalyzeResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
			Config: DMTaskConfig{},
		})
		return
	}
	defer file.Close()

	// read all of the contents of our uploaded file into a
	// byte array
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		p.genJSONResp(w, http.StatusBadRequest, AnalyzeResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
			Config: DMTaskConfig{},
		})
		return
	}

	cfg := &DMTaskConfig{}
	if err = yaml.Unmarshal(fileBytes, cfg); err != nil {
		log.L().Error("unmarshal config data failed", zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, AnalyzeResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
			Config: DMTaskConfig{},
		})
		return
	}

	if err = cfg.Verify(); err != nil {
		log.L().Error("verify config failed", zap.Reflect("config", cfg), zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, GenerateConfigResult{
			CommonResult: CommonResult{
				Result: failed,
				Error:  err.Error(),
			},
			Filepath: "",
		})
		return
	}

	log.L().Info("analyze config", zap.String("config name", cfg.Name))

	// decrypt password
	cfg.TargetDB.Password = utils.DecryptOrPlaintext(cfg.TargetDB.Password)

	p.genJSONResp(w, http.StatusOK, AnalyzeResult{
		CommonResult: CommonResult{
			Result: success,
			Error:  "",
		},
		Config: *cfg,
	})
}

// Download returns dm task config data for generate config file.
func (p *Handler) Download(w http.ResponseWriter, req *http.Request) {
	log.L().Info("receive Download request")

	vars := req.URL.Query()
	filepaths, ok := vars["filepath"]
	if !ok {
		log.L().Error("can't find argument 'filepath'")
		p.genJSONResp(w, http.StatusBadRequest, nil)
		return
	}
	log.L().Info("download task config file", zap.Strings("file paths", filepaths))

	if len(filepaths) != 1 {
		log.L().Error("wrong url query", zap.Reflect("query", vars))
		p.genJSONResp(w, http.StatusBadRequest, nil)
		return
	}
	filepath := filepaths[0]

	if !p.fileValid(filepath) {
		log.L().Info("file is not found or invalid", zap.String("file path", filepath))
		p.genJSONResp(w, http.StatusNotFound, nil)
		return
	}

	cfgData, err := os.ReadFile(filepath)
	if err != nil {
		log.L().Error("read file failed", zap.String("filepath", filepath), zap.Error(err))
		p.genJSONResp(w, http.StatusBadRequest, nil)
		return
	}

	// Generate the server headers
	fileSize := len(string(cfgData))
	w.Header().Set("Content-Type", "text/yaml")
	w.Header().Set("Content-Disposition", "attachment; filename="+path.Base(filepath))
	w.Header().Set("Expires", "0")
	w.Header().Set("Content-Transfer-Encoding", "binary")
	w.Header().Set("Content-Length", strconv.Itoa(fileSize))
	w.Header().Set("Content-Control", "private, no-transform, no-store, must-revalidate")

	http.ServeContent(w, req, path.Base(filepath), time.Now(), bytes.NewReader(cfgData))
}

// fileValid judge the download file path is valid or not.
func (p *Handler) fileValid(filepath string) bool {
	if dir := path.Dir(filepath); dir != p.path {
		return false
	}

	if !strings.HasSuffix(filepath, "yaml") {
		return false
	}

	return true
}

// genJSONResp marshals the given interface object and writes the JSON response.
func (p *Handler) genJSONResp(w io.Writer, status int, v interface{}) {
	if err := p.r.JSON(w, status, v); err != nil {
		errStr := fmt.Sprintf("generate json response for %v failed %v", v, err)
		log.L().Error("", zap.String("error", errStr))
		//nolint:errcheck
		io.WriteString(w, errStr)
	}
}

var getDBConnFunc = getDBConnFromReq

func getDBConnFromReq(req *http.Request, timeout int) (*sql.DB, string, error) {
	dbCfg := &DBConfig{}
	if err := readJSON(req.Body, dbCfg); err != nil {
		return nil, "", errors.Trace(err)
	}

	db, err := openDB(*dbCfg, timeout)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	return db, fmt.Sprintf("%s:%d", dbCfg.Host, dbCfg.Port), nil
}

func readJSON(r io.Reader, data interface{}) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return errors.Trace(err)
	}
	err = json.Unmarshal(b, data)
	return errors.Trace(err)
}

func adjustConfig(cfg *DMTaskConfig) error {
	// config from front-end will not fill FilterRules, RouteRules and BAListName
	// in mysql instance, need fill them by analyze rule's name
	filterRules := make(map[string][]string)
	routeRules := make(map[string][]string)
	baList := make(map[string]string)
	mydumperCfg := make(map[string]string)
	cfg.Mydumpers = make(map[string]*config.MydumperConfig)
	// used to judge is-sharding
	routeRuleMap := make(map[string]interface{})

	for name := range cfg.Filters {
		sourceID, _, err := analyzeRuleName(name)
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := filterRules[sourceID]; !ok {
			filterRules[sourceID] = make([]string, 0, 5)
		}

		filterRules[sourceID] = append(filterRules[sourceID], name)
	}

	for name, rule := range cfg.Routes {
		sourceID, _, err := analyzeRuleName(name)
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := routeRules[sourceID]; !ok {
			routeRules[sourceID] = make([]string, 0, 5)
		}

		routeRules[sourceID] = append(routeRules[sourceID], name)

		// if more than one table route to the same target table, is-sharding should be true
		tableStr := fmt.Sprintf("%s|%s", rule.TargetSchema, rule.TargetTable)
		if _, ok := routeRuleMap[tableStr]; ok {
			cfg.IsSharding = true
		} else {
			routeRuleMap[tableStr] = struct{}{}
		}
	}

	for name, rule := range cfg.BAList {
		sourceID, _, err := analyzeRuleName(name)
		if err != nil {
			return errors.Trace(err)
		}

		baList[sourceID] = name

		mydumperCfgName := generateMydumperCfgName(sourceID)
		mydumperCfg[sourceID] = mydumperCfgName
		cfg.Mydumpers[mydumperCfgName] = generateMydumperCfg(rule)
	}

	for _, instance := range cfg.MySQLInstances {
		if rulesName, ok := filterRules[instance.SourceID]; ok {
			instance.FilterRules = rulesName
		}
		if rulesName, ok := routeRules[instance.SourceID]; ok {
			instance.RouteRules = rulesName
		}
		if ruleName, ok := baList[instance.SourceID]; ok {
			instance.BAListName = ruleName
		}
		if dumpCfgName, ok := mydumperCfg[instance.SourceID]; ok {
			instance.MydumperConfigName = dumpCfgName
		}
	}

	// encrypt password
	enPwd, err := utils.Encrypt(cfg.TargetDB.Password)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.TargetDB.Password = enPwd

	return nil
}

// rule name looks like "instance1.route_rules.1".
func analyzeRuleName(name string) (sourceID string, tp string, err error) {
	items := strings.Split(name, ".")
	if len(items) != 3 {
		return "", "", errors.Errorf("rules name %s is invalid", name)
	}

	switch items[1] {
	case routeTp, filterTp, baListTp:
	default:
		return "", "", errors.Errorf("rules name %s is invalid", name)
	}

	return items[0], items[1], nil
}

func generateMydumperCfg(baList *filter.Rules) *config.MydumperConfig {
	tables := make([]string, 0, len(baList.DoTables))

	for _, table := range baList.DoTables {
		tables = append(tables, fmt.Sprintf("%s.%s", table.Schema, table.Name))
	}

	extraArgs := ""
	if len(tables) != 0 {
		extraArgs = fmt.Sprintf("-T %s", strings.Join(tables, ","))
	}

	return &config.MydumperConfig{
		MydumperPath:  "bin/mydumper",
		Threads:       4,
		ChunkFilesize: "64",
		SkipTzUTC:     true,
		ExtraArgs:     extraArgs,
	}
}

func generateMydumperCfgName(sourceID string) string {
	return fmt.Sprintf("%s.dump", sourceID)
}

func generateTaskFileName(taskName string) string {
	return fmt.Sprintf("%s-task.yaml", taskName)
}

// openDB opens a mysql connection FD.
func openDB(cfg DBConfig, timeout int) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&timeout=%ds", cfg.User, cfg.Password, cfg.Host, cfg.Port, timeout)

	dbConn, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = dbConn.Ping()
	return dbConn, errors.Trace(err)
}

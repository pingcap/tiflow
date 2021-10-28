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

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	datasourceName = "dm-cluster"
	outputDir      = "./dashboards"

	standardName     = "DM-Monitor-Standard.json"
	professionalName = "DM-Monitor-Professional.json"
)

// dashboard file name -> title.
var dashboards = map[string]string{
	standardName:     "Test-Cluster-DM",
	professionalName: "Test-Cluster-DM",
}

func readDashboard(dir string, name string) (string, error) {
	file := filepath.Join(dir, name)
	data, err := os.ReadFile(file)
	if err != nil {
		return "", errors.Errorf("File %s do not exists", file)
	}
	return string(data), nil
}

func writeDashboard(dir string, name string, body string) error {
	title, exist := dashboards[name]
	if !exist {
		return errors.Errorf("%s dashboard is not found in operator", name)
	}

	writeFile(dir, name, filterDashboard(body, title))
	return nil
}

func writeFile(baseDir string, fileName string, body string) {
	if body == "" {
		return
	}

	fn := filepath.Join(baseDir, fileName)
	f, err := os.Create(fn)
	checkErr(err, "create file failed, f="+fn)
	defer f.Close()

	if _, err := f.WriteString(body); err != nil {
		checkErr(err, "write file failed, f="+fn)
	}
}

func checkErr(err error, msg string) {
	if err != nil {
		panic(errors.Wrap(err, msg))
	}
}

func filterDashboard(str string, title string) string {
	// replace grafana item
	var err error
	r := gjson.Get(str, "__requires.0.type")
	if r.Exists() && r.Str == "grafana" {
		str, err = sjson.Set(str, "__requires.0.version", "")
		checkErr(err, "update links filed failed")
	}
	// replace links item
	if gjson.Get(str, "links").Exists() {
		str, err = sjson.Set(str, "links", []struct{}{})
		checkErr(err, "update links failed")
	}

	// replace datasource name
	if gjson.Get(str, "__inputs").Exists() && gjson.Get(str, "__inputs.0.name").Exists() {
		datasource := gjson.Get(str, "__inputs.0.name").Str
		str = strings.ReplaceAll(str, fmt.Sprintf("${%s}", datasource), datasourceName)
	}

	// delete input definition
	if gjson.Get(str, "__inputs").Exists() {
		str, err = sjson.Delete(str, "__inputs")
		checkErr(err, "delete path failed")
	}

	// unify the title name
	str, err = sjson.Set(str, "title", title)
	checkErr(err, "replace title failed")

	return str
}

func main() {
	str, err := readDashboard(outputDir, standardName)
	checkErr(err, "read standar dashboard file failed")
	checkErr(writeDashboard(outputDir, standardName, str), "write standard dashboard file failed")

	str, err = readDashboard(outputDir, professionalName)
	checkErr(err, "read professional dashboard file failed")
	checkErr(writeDashboard(outputDir, professionalName, str), "write professional dashboard file failed")
}

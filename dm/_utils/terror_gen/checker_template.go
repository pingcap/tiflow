// Copyright 2019 PingCAP, Inc.
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
	"bufio"
	"bytes"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

const (
	developErrorFile = "errors_develop.txt"
	releaseErrorFile = "errors_release.txt"
	checkerFile      = "{{.CheckerFile}}"
	tomlErrorFile    = "../../errors.toml"
)

var dumpErrorRe = regexp.MustCompile("^([a-zA-Z].*),\\[code=([0-9]+).*$")

type tomlErrorBody = struct {
	Message     string   `toml:"message"`
	Description string   `toml:"description"`
	Workaround  string   `toml:"workaround"`
	Tags        []string `toml:"tags"`
}

type tomlErrorItem = struct {
	key  string
	code terror.ErrCode
	body string
}

// used to generate `errors.toml` in the repo's root.
var tomlErrors []tomlErrorItem

var errors = []struct {
	name string
	err  *terror.Error
}{
	// sample:
	// {"ErrWorkerExecDDLTimeout", terror.ErrWorkerExecDDLTimeout},
	// {{.ErrList}}
}

func genErrors() {
	f, err := os.Create(developErrorFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, item := range errors {
		s := strings.SplitN(item.err.Error(), " ", 2)
		if len(s) > 1 {
			// errName,[code:class:scope:level], "Message, RawCause, Workaround"
			w.WriteString(fmt.Sprintf("%s,%s \"%s\"\n", item.name, s[0], strings.ReplaceAll(s[1], "\n", "\\n")))
		} else {
			// errName,[code:class:scope:level]
			w.WriteString(fmt.Sprintf("%s,%s\n", item.name, s[0]))
		}

		body := tomlErrorBody{
			Message:     item.err.Message(),
			Description: "", // empty now
			Workaround:  item.err.Workaround(),
			Tags:        []string{item.err.Scope().String(), item.err.Level().String()},
		}
		var buf bytes.Buffer
		enc := toml.NewEncoder(&buf)
		err := enc.Encode(body)
		if err != nil {
			panic(err)
		}

		tomlErrors = append(tomlErrors, tomlErrorItem{
			key:  fmt.Sprintf("error.DM-%s-%d", item.err.Class(), item.err.Code()),
			code: item.err.Code(),
			body: buf.String(),
		})
	}
	w.Flush()

	// sort according to the code.
	sort.Slice(tomlErrors, func(i, j int) bool {
		return tomlErrors[i].code < tomlErrors[j].code
	})
}

func readErrorFile(filename string) map[string]int64 {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	result := make(map[string]int64)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		s := scanner.Text()
		match := dumpErrorRe.FindStringSubmatch(s)
		if len(match) != 3 {
			panic(fmt.Sprintf("invalid error: %s", s))
		}
		code, err := strconv.ParseInt(match[2], 10, 64)
		if err != nil {
			panic(err)
		}
		result[match[1]] = code
	}
	return result
}

func compareErrors() bool {
	changedErrorCode := make(map[string][]int64)
	duplicateErrorCode := make(map[int64][]string)
	release := readErrorFile(releaseErrorFile)
	dev := readErrorFile(developErrorFile)

	for name, code := range dev {
		if releaseCode, ok := release[name]; ok && code != releaseCode {
			changedErrorCode[name] = []int64{releaseCode, code}
		}
		if _, ok := duplicateErrorCode[code]; ok {
			duplicateErrorCode[code] = append(duplicateErrorCode[code], name)
		} else {
			duplicateErrorCode[code] = []string{name}
		}
	}
	for code, names := range duplicateErrorCode {
		if len(names) == 1 {
			delete(duplicateErrorCode, code)
		}
	}

	// check each non-new-added error in develop version has the same error code with the release version
	if len(changedErrorCode) > 0 {
		os.Stderr.WriteString("\n************ error code not same with the release version ************\n")
	}
	for name, codes := range changedErrorCode {
		fmt.Fprintf(os.Stderr, "name: %s release code: %d current code: %d\n", name, codes[0], codes[1])
	}

	// check each error in develop version has a unique error code
	if len(duplicateErrorCode) > 0 {
		os.Stderr.WriteString("\n************ error code not unique ************\n")
	}
	for code, names := range duplicateErrorCode {
		fmt.Fprintf(os.Stderr, "code: %d names: %v\n", code, names)
	}

	return len(changedErrorCode) == 0 && len(duplicateErrorCode) == 0
}

func cleanup(success bool) {
	if success {
		if err := os.Rename(developErrorFile, releaseErrorFile); err != nil {
			panic(err)
		}

		tef, err := os.Create(tomlErrorFile)
		if err != nil {
			panic(err)
		}
		defer tef.Close()
		for _, item := range tomlErrors {
			// generate to TOML file, poor man's method for ordered codes.
			_, err = tef.WriteString(fmt.Sprintf("[%s]\n", item.key))
			if err != nil {
				panic(err)
			}
			_, err = tef.WriteString(item.body)
			if err != nil {
				panic(err)
			}
			_, err = tef.WriteString("\n")
			if err != nil {
				panic(err)
			}
		}

		fmt.Println("check pass")
	} else {
		if err := os.Remove(developErrorFile); err != nil {
			panic(err)
		}
		os.Exit(1)
	}
}

func main() {
	defer func() {
		os.Remove(checkerFile)
	}()
	genErrors()
	cleanup(compareErrors())
}

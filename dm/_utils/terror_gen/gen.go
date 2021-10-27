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
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"text/template"
)

const (
	templateCheckerFile  = "checker_template.go"
	generatedCheckerFile = "checker_generated.go"
)

var templatePlaceHolder = []byte("// TODO: fillin me")

func getErrorInstances(filepath string) []string {
	fset := token.NewFileSet()
	parserMode := parser.ParseComments
	var (
		fileAst *ast.File
		err     error
		result  = make([]string, 0, 100)
	)

	fileAst, err = parser.ParseFile(fset, filepath, nil, parserMode)
	if err != nil {
		panic(err)
	}

	for _, d := range fileAst.Decls {
		switch decl := d.(type) {
		case *ast.GenDecl:
			specs := decl.Specs
			for _, spec := range specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				name := vs.Names[0].Name
				if strings.HasPrefix(name, "Err") {
					result = append(result, name)
				}
			}
		}
	}
	return result
}

func genFile(names []string) {
	var (
		buf, genBuf bytes.Buffer
		content     []byte
		err         error
	)
	content, err = os.ReadFile(templateCheckerFile)
	if err != nil {
		panic(err)
	}
	fmt.Fprint(&buf, "\n")
	for _, name := range names {
		fmt.Fprintf(&buf, "\t{\"%s\", terror.%s},\n", name, name)
	}

	data := map[string]interface{}{
		"ErrList":     buf.String(),
		"CheckerFile": generatedCheckerFile,
	}
	t := template.Must(template.New(generatedCheckerFile).Parse(string(content)))
	err = t.Execute(&genBuf, data)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(generatedCheckerFile, genBuf.Bytes(), 0644)
	if err != nil {
		panic(err)
	}
}

func main() {
	names := getErrorInstances(os.Args[1])
	genFile(names)
}

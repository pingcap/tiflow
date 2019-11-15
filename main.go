package main

import (
	"github.com/pingcap/ticdc/cmd"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func main() {
	cmd.Execute()
}

package main

import (
	"github.com/pingcap/tidb-cdc/cmd"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func main() {
	cmd.Execute()
}

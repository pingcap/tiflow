package main

import (
	"fmt"
	"github.com/pingcap/ticdc/cdc/sink/publicUtils"
)

func main() {
	//导入配置文件
	configMap := publicUtils.InitConfig("configuration.txt")
	//configMap := publicUtils.InitConfig("ticdc.toml")
	//获取配置里host属性的value
	//fmt.Println(configMap["host"])
	//查看配置文件里所有键值对
	fmt.Println(configMap)
}

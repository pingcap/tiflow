package main

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/integration/framework"
	"go.uber.org/zap/zapcore"
)

func main() {
	log.SetLevel(zapcore.DebugLevel)
	env := framework.NewAvroKafkaDockerEnv()
	env.Setup()
	//env.RunTest(NewSimple())
	env.RunTest(NewHelperCase())
	env.TearDown()
}

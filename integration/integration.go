package main

import "github.com/pingcap/ticdc/integration/framework"

func main() {
	env := framework.NewAvroKafkaDockerEnv()
	env.Setup()
	env.RunTest(NewSimple())
	env.TearDown()
}

# Integration Framework

## Introduction
The **Integration Framework** is designed to provide a flexible way for contributors to write integration tests for new sinks or MQ protocols. The core of the framework is stored in `{ticdc_root}/integration/framework`, and test cases should be stored in `{ticdc_root}/integration`. Currently, although the Framework is still under active development, it is capable of helping test Avro support and it is the only officially supported way for developers to run integration tests with Kafka connect.

## Quick Start
To create a test case, you need to:
- create a struct that implements the `Task` interface,
- and ask the Environment to run the task in the `main` function in `integration.go`.
Note that the second step will be automated soon.

```go
// Task represents a single test case
type Task interface {
	Name() string
	GetCDCProfile() *CDCProfile
	Prepare(taskContext *TaskContext) error
	Run(taskContext *TaskContext) error
}
```

For the time being, if you would like to write Avro tests, it is recommended to embed `framework.AvroSingleTableTask` in your own struct, which executes the necessary setup steps, including creating the Kafka Connect sink and creating the changefeed with appropriate configurations.


Example:
```go
type myCase struct {
	framework.AvroSingleTableTask
}

func newMyCase() *myCase{
	myCase := new(myCase)
	myCase.AvroSingleTableTask.TableName = "test"
	return myCase
}

func (c *myCase) Name() string {
	return "My Case"
}

func (c *alterCase) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int primary key, value int)")
	if err != nil {
		return err
	}

	// Get a handle of an existing table
	table := ctx.SQLHelper().GetTable("test")
	// Create an SQL request, send it to the upstream, wait for completion and check the correctness of replication
	err = table.Insert(map[string]interface{}{
		"id":    0,
		"value": 0,
	}).Send().Wait().Check()
	if err != nil {
		return errors.AddStack(err)
	}

	// To wait on a batch of SQL requests, create a slice of Awaitables
	reqs := make([]framework.Awaitable, 0)
	for i := 1; i < 1000; i++ {
		// Only send, do not wait
		req := table.Insert(map[string]interface{}{
			"id":    i,
			"value": i,
		}).Send()
		reqs = append(reqs, req)
	}

	// Wait on SQL requests in batch and check the correctness
	return framework.All(ctx.SQLHelper(), reqs).Wait().Check()
}
```

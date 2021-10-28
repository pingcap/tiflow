# Proposal: Improve Error System

- Author(s):    [yangfei](https://github.com/amyangfei)
- Last updated: 2019-07-22

## Abstract

This proposal introduces an error code mechanism in the DM error system, making a regulation for error handling.

Table of contents:

- [Background](#Background)
- [Implementation](#Implementation)
    - [Error object definition](#Error-object-definition)
    - [Error classification and error codes](#Error-classification-and-error-codes)
    - [Goal of error handling](#Goal-of-error-handling)
        - [Provide better error equivalences check](#Provide-better-error-equivalences-check)
        - [Enhance the error chain](#Enhance-the-error-chain)
        - [Embedded stack traces](#Embedded-stack-traces)
        - [Error output specification](#Error-output-specification)
    - [Error handling regulation](#Error-handling-regulation)
        - [API list](#API-list)

## Background

Currently all DM errors are constructed by `errors.Errorf` with error description or `errors.Annotatef` with description and annotated error. Description oriented error is easy for users to understand, however when users report error to developer or DBA, they have to give the full description of the error, which is often a long text with some embed programming objects. On the other side, if users want to find some specific error, they have to grep some keywords or even a long text from the log. From the developers’ perspective, we need a better way to distinguish specific error rather than matching error relying on the presence of a substring in the error message. Based on the above considerations, it is highly demanded to design a new error system, which will provide better error classification, more useful embedded information and better error equivalences check. This proposal will focus on the following points:

- Organize DM error scenarios and classify these errors
- Provide a unified error code mechanism
- Standardize error handling, including how to create, propagate and log an error

## Implementation

### Error object definition

Error object is defined as below, with some import fields:

- code: error code, unique for each error type
- class: error class based on the belonging component or unit, etc; classified by code logic
- scope: the scope within which this error happens, including upstream, downstream, and DM inner
- level: emergency level of this error, including high, medium, and low
- args: variables used for error message generation. For example, we have an error `ErrFailedFlushCheckpoint = terror.Syncer.New(5300, “failed to flush checkpoint %s”)`.  We can use this error as ErrFailedFlushCheckpoint.GenWithArgs(checkpoint), so we don’t need additional error messages when we use this error
- rawCause: used to record root errors via a third party function call
- stack: thanks to [pingcap/errors/StackTracer](https://github.com/pingcap/errors/blob/dc8ffe785c7fc9a74eeb5241814d77f1c5fb5e58/stack.go#L13-L17), we can use this to record stack trace easily

```go
type ErrCode int
type ErrClass int
type ErrScope int
type ErrLevel int

type Error struct {
	code		ErrCode
	class		ErrClass
	scope		ErrScope
	level		ErrLevel
	message		string
	args		[]interface{}
	rawCause	error
	stack		errors.StackTracer
}
```

### Error classification and error codes

1. Errors are classfied by the class field, which relates to the code logic
2. Error codes range allocation will be added later

### Goal of error handling

#### Provide better error equivalences check

To provide better error equivalences check, we need to do the following:
- Enable fast, reliable, and secure determination of whether a particular error cause is present（no relying on the presence of a substring in the error messages）
- Support protobuf-encodable error object，so we can work with errors transmitted across the network via GRPC.（TODO）
- Provide the following interface for error equivalences check.

```go
// Equal returns true iff the error contains `reference` in any of its
func (e *Error) Equal(reference error) bool {}

// EqualAny is like Equal() but supports multiple reference errors.
func (e terror) EqualAny(references ...error) bool {}
```

#### Enhance the error chain

1. When we generate a new error in DM level source, we always use `Generate` or `Generatef` to create a new Error instance from a defined error list.
2. When we invoke a third party function and get an error, we should change this error to adapt to our error system. We have two choices here:

- Keep the error message from the third party function and create a related error instance in our new error system.
- Create a new Error instance, and save the third party error in its `rawCause` field.

3. Supposing one function A invokes another function B, and function A is also invoked by other code, both functions A and function B are DM level code and have an error field in their return values, we should make a rule about how to propagate error to upper code. In this scenario, we call function A as current function, we call function B as inner function, and we call the code invokes function A as upper code stack. The inner function returns `err != nil`, the current function shall propagate this error to the upper code stack, it will generate different error object based on the code logic.

- If the error information returned from the inner function is enough for the current function to describe the error scenario, then it returns `errors.Trace(err)` or `err` directly to the upper code stack.
- If more error information is required for the current function, such as more detail descriptions, some variable values from the current code stack, then it `Annotate`s the error returned from the inner function or even changes fields such as `ErrClass`, `ErrLevel`, etc. Take the following code snippet as an example, the checkpoint `FlushPointsExcept` returns an error, and then in Syncer’s code stack, it annotates the returned error with more information.


```go
func (s *Syncer) flushCheckPoints() error {
	err := s.checkpoint.FlushPointsExcept(...)
	if err != nil {
		return Annotatef(err, "flush checkpoint %s", s.checkpoint)
	}
}
```

- In the following code logic, the `txn.Exec` encounters error `err1`, and we try to roll back the transaction but unfortunately the `txn.Rollback` gets another error `err2`. In this scenario, `err1` is more essential than err2. We are considering adding a secondary error in the error instance, but not in the current version. We should use errors carefully in this scenario.

```go
err1 := txn.Exec(sql, args...)
if err != nil {
	err2 := txn.Rollback()
	if err2 != nil {
		log.Errorf("rollback error: %v", err2)
	}
	// should return the exec err1, instead of the rollback err2.
	return errors.Trace(err1)
}
```

As for error combination requirements, we provide the following APIs.

```go
// Annotate adds a message and ensures there is a stack trace
func Annotate(err error, message string) error {}

// Annotatef adds a message and ensures there is a stack trace
func Annotatef(err error, format string, args ...interface{}) error {}

// Delegate creates a new *Error with the same fields of the given *Error,
// except for new arguments, it also sets the err as raw cause of *Error
func (e *Error) Delegate(err error, args ...interface{}) error {}
```

Differences between `error Annotate` and `error Delegate`

- The Annotate way asserts the error to an `*Error` instance and adds an additional error message.
- The Delegate way creates a new `*Error` instance from the given `*Error`, and sets the given error to its `rawCause` field. The error in the parameter is often returned from a third party function and we store it for using later.

#### Embedded stack traces

We use [pingcap/errors/StackTracer](https://www.google.com/url?q=https://github.com/pingcap/errors/blob/master/stack.go%23L13-L17&sa=D&ust=1563783859473000) to record the stack trace, and ensures that stack trace information is added each time when we create a new Error instance. In addition, we should keep the stack trace in the backtracing of the function call. Let’s see how each way keeps the stack trace.

-  Create an `*Error` for the first time: `Generate`, `Generatef` or `Delegate` automaticity adds the stack trace
- Get an `*Error` from the DM function, `return err` directly: the stack trace is kept in the *Error instance.
- Get an `*Error` from the DM function, use `Annotate` or `Annotatef` to change some fields of the `*Error`: we still use the original `*Error` instance, and only change fields excluding `code` and `stack`, so the stack trace is kept.

#### Error output specification

- how to log error in the log file
- how to display error message in dmctl response

## Error handling regulation

- When we generate an error in DM for the first time, we should always use the new error API, including `Generate`, `Generatef`, and `Delegate`
- When we want to generate an error based on a third-party error, `Delegate` is recommended
- There are two ways to handle errors in the DM function call stack: one way is to return the error directly, the other way is to `Annotate` the error with more information
- DO NOT use other error libraries anymore, such as [pingcap/errors](https://www.google.com/url?q=https://github.com/pingcap/errors&sa=D&ust=1563783859475000) to wrap or add stack trace with the error instance in our new error system, which may lead to stack trace missing before this call and unexpected error format.
- We should try our best to wrap the proper ErrClass to all errors with ErrClass ClassFunctional, which will help user to find out the error is happened in which component, module or use scenario.

### API list

```go
// Equal returns true if the error contains `reference` in any of its
func (e *Error) Equal(reference error) bool {}

// EqualAny is like Equal() but supports multiple reference errors.
func (e terror) EqualAny(references ...error) bool {}

// Generate generates a new *Error with the same class and code, and new arguments.
func (e *Error) Generate(args ...interface{}) error {}

// Generatef generates a new *Error with the same class and code, and a new formatted message.
func (e *Error) Generatef(format string, args ...interface{}) error {}

// Annotate adds a message and ensures there is a stack trace
func Annotate(err error, message string) error {}

// Annotatef adds a message and ensures there is a stack trace
func Annotatef(err error, format string, args ...interface{}) error {}

// Delegate creates a new *Error with the same fields of the give *Error,
// except for new arguments, it also sets the err as raw cause of *Error
func (e *Error) Delegate(err error, args ...interface{}) error {}
```

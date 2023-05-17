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

// Package actor provides a simple actor system. It's a framework that can poll
// many actors concurrently.
//
// The following diagram shows how a system polls actors.
//
//	,------.          ,-------.    ,-----.           ,------.          ,-----.
//	|Router|          |Mailbox|    |ready|           |System|          |Actor|
//	`--+---'          `---+---'    `--+--'           `--+---'          `--+--'
//	   |----.             |           |                 |                 |
//	   |    | Send(msgs)  |           |                 |                 |
//	   |<---'             |           |                 |                 |
//	   |                  |           |                 |                 |
//	   |----.             |           |                 |                 |
//	   |    | find proc   |           |                 |                 |
//	   |<---'             |           |                 |                 |
//	   |                  |           |                 |                 |
//	   |  proc            |           |                 |                 |
//	   |    .Mailbox      |           |                 |                 |
//	   |    .Send(msgs)   |           |                 |                 |
//	   | ---------------->|           |                 |                 |
//	   |                  |           |                 |                 |
//	   |           schedule(proc)     |                 |                 |
//	   | ---------------------------->|                 |                 |
//	   |                  |           |                 |                 |
//	   |                  |           |--.              |                 |
//	   |                  |           |  | enqueue(proc)|                 |
//	   |                  |           |<-'              |                 |
//	   |                  |           |                 |                 |
//	   |                  |           |    signal()     |                 |
//	   |                  |           |---------------->|                 |
//	   |                  |           |                 |                 |
//	   |                  |           |   fetchProc()   |                 |
//	   |                  |           |<-----------------                 |
//	   |                  |           |                 |                 |
//	   |                  |           |   return proc   |                 |
//	   |                  |           |---------------->|                 |
//	   |                  |           |                 |                 |
//	   |                  |           |                 | poll proc.Actor |
//	   |                  |           |                 | --------------->|
//	   |                  |           |                 |                 |
//	   |                  |           |   tryReceive    |                 |
//	   |                  |<----- ----------------------------------------|
//	   |                  |           |                 |                 |
//	   |                  |           |  return msgs    |                 |
//	   |                  |------ --------------------------------------->|
//	   |                  |           |                 |                 | Poll(msgs)
//	   |                  |           |                 |                 |----.
//	   |                  |           |                 |                 |    |
//	   |                  |           |                 |                 |<---'
//	,--+---.          ,---+---.    ,--+--.           ,--+---.          ,--+--.
//	|Router|          |Mailbox|    |ready|           |System|          |Actor|
//	`------'          `-------'    `-----'           `------'          `-----'
//
// See docs/actor-system.svg for the relationship about System, Actor
// Mailbox and ready.
package actor

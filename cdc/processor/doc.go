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

/*
Package processor implements the processor logic based on ETCD worker(pkg/orchestrator).

There are three main modules: Manager, Processor and TablePipeline(cdc/processor/pipeline).
The Manager's main responsibility is to maintain the Processor's life cycle, like create and destroy the processor instances.
The Processor's main responsibility is to maintain the TablePipeline's life cycle according to the state stored by ETCD,
and calculate the local resolved TS and local checkpoint Ts and put them into ETCD.
The TablePipeline listens to the kv change logs of a specified table(with its mark table if it exists), and sends logs to Sink After sorting and mounting.

The relationship between the three module is as follows:

One Capture(with processor role)  -> Processor Manager -> Processor(changefeed1) -> TablePipeline(tableA)

	╲                         ╲
	 ╲                         -> TablePipeline(tableB)
	  ╲
	   ╲
	    -> Processor(changefeed2) -> TablePipeline(tableC)
	                              ╲
	                               -> TablePipeline(tableD)
*/
package processor

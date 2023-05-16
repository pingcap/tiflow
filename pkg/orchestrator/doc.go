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
Package orchestrator mainly implements a ETCD worker.
A ETCD worker is used to read/write data from ETCD servers based on snapshot and data patches.
Here is a detailed description of how the ETCD worker works:

				   ETCD Servers
					|       ^
					|       |
		   1. Watch |       | 5. Txn
					|       |
					v       |
				    EtcdWorker
					|       ^
					|       |
		   2. Update|       | 4. DataPatch
		   +--------+       +-------+
		   |                        |
		   |                        |
		   v         3.Tick         |
		ReactorState ----------> Reactor

	 1. EtcdWorker watches the txn modification log from ETCD servers
	 2. EtcdWorker updates the txn modification listened from ETCD servers by calling the Update function of ReactorState
	 3. EtcdWorker calls the Tick function of Reactor, and EtcdWorker make sure the state of ReactorState is a consistent snapshot of ETCD servers
	 4. Reactor is implemented by the upper layer application. Usually, Reactor will produce DataPatches when the Tick function called
	    EtcdWorker apply all the DataPatches produced by Reactor
	 5. EtcdWorker commits a txn to ETCD according to DataPatches

The upper layer application which is a user of EtcdWorker only need to implement Reactor and ReactorState interface.
The ReactorState is used to maintenance status of ETCD, and the Reactor can produce DataPatches differently according to the ReactorState.
The EtcdWorker make sure any ReactorState which perceived by Reactor must be a consistent snapshot of ETCD servers.
*/
package orchestrator

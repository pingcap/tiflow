# Distributed Scheduling in TiCDC

## Background

TiCDC boasts high availability and horizontal scalability. To make this possible, TiCDC needs a distributed scheduling mechanism, a mechanism by which tables can be distributed across the nodes in the cluster and sustain failures of nodes. We still need a center of control, which we call the Owner, but the Owner should fail over very quickly should the old one fails.

In the beginning of the TiCDC project, we chose a solution that sends all information over Etcd, which is a distributed key-value store. However, this solution has proven to be problematic as it fails to scale both horizontally and vertically. As a solution, we created a solution using direct peer-to-peer messages, which not only has semantics better suited to scheduling, but also performs and scales better.

## The Abstraction

To succinctly describe the algorithm used here, we need to abstract the TiCDC Owner and Processor. To simplify the matter, we will omit "changefeed" management here, and suppose that multiple "changefeeds" are isolated from each other as far as scheduling is concerned (which they basically are).

- The Owner is a piece of code that can persist and restores a timestamp `global checkpoint`, which is guaranteed to be monotonically increasing and a lower bound of the progresses of all nodes on all tables. The Owner will call our `ScheduleDispatcher` periodically and supply it with both the latest `global checkpoint` and a list of tables that should currently be replicating.
- The Processor is a piece of code that actually replicates the tables. We can `Add` and `Remove` tables from it, and query it about the status of a table.

## The Protocol

The communication protocol between the Owner and the Processors is as follows:

|   Name	          |   Direction	|   Schema	|   Notes	|
|---	              |---	        |---	    |---	    |
|   DispatchTable	  |Owner->Processor|   	|   	|
|   DispatchTableResponse	|Processor->Owner|   	|   	|
|   Announce	|Owner->Processor|   	|   	|
|   Sync	|Processor->Owner|   	|   	|
|   Checkpoint	|Processor->Owner|   	|   	|




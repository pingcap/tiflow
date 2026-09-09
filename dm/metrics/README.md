# Task-associated worker CPU

`dm_task_worker_cpu_usage` is a gauge exposing the CPU usage of the **worker
process hosting a task**, not CPU usage attributed to that task. It reuses the
same sample as `dm_worker_cpu_usage`, refreshed every 10 seconds. A value of `100`
means one fully utilized CPU core; values can exceed `100`. It is not normalized
to the container's CPU quota. Do not apply `rate()` or multiply it by `100`.

The metric has the fixed labels `task`, `source_id`, and `worker`. Each scrape
derives its series from `dm_worker_task_state`, so new, running, and paused tasks
are represented. Stopped or finished subtasks disappear when their task-state
series are removed. There is no separate CPU series cache to clean up.

## Custom labels and queries

The existing task metric-label mechanism also applies to this metric. For
example, add the following to the task YAML configuration:

```yaml
metric-labels:
  keyspace_name: ks1
```

The equivalent OpenAPI Task field is `metric_labels`. `keyspace_name` is an
example custom label, not a built-in field or a value inferred from a keyspace ID.
No additional worker configuration is needed.

Query the worker CPU associated with tasks carrying that label:

```promql
dm_task_worker_cpu_usage{keyspace_name="ks1"}
```

## Shared-worker semantics

All tasks on the same worker report the same worker CPU sample. Do not sum these
series across tasks: that would count a shared worker multiple times. To display
one series per worker for the selected keyspace, for example:

```promql
max by (job, instance, worker, keyspace_name) (
  dm_task_worker_cpu_usage{keyspace_name="ks1"}
)
```

Use the actual scrape identity labels and retain cluster identifiers when
querying multiple clusters. Deduplication does not attribute CPU to a task or
tenant. The value includes other tasks and shared process work such as GC and
relay; routing it with a tenant label still exposes the whole worker's usage.

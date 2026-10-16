# DM diagnostic redaction

Set `redact-info-log = true` in the configuration of **each DM-master and
DM-worker**, or pass `--redact-info-log=true`, then restart those processes.
The default is `false`. Master and worker are separate processes; enabling the
option on one does not configure the others.

When enabled, annotated SQL, DDL, row images, primary/unique key values,
validator comparison values, and query arguments are replaced by `?` in logs.
Free-form error fields are hidden because database errors can quote row values
and binlog decode errors can include event bytes. Typed errors retain their
identifiers where available. Debug-level embedded TiDB slow-query logging is
disabled. Task names, table names, binlog positions, progress, and counters
remain available at their ordinary diagnostic sites.

Worker status responses also mask error text and blocking DDLs so the master
cannot log those values again. This changes the error text shown by
`query-status`, but keeps error codes, task stages, positions, and progress.
Workers retain the original internal errors for retry and resume decisions.
Queries sent to databases, replication data, checkpoints, and validator error
records are not changed. Explicit validator error-data retrieval still returns
the recorded comparison data; it is a data inspection operation, not a log.

Database passwords and TLS contents in start-task, check-task, source-operation,
and subtask-update-validation request logs are always masked, independently of
this option. Proxy URL credentials are also always
hidden, including in dmctl startup output. `get-config` omits TLS contents in
both byte and Base64 forms while retaining certificate paths. Request
configuration logs preserve ordinary configuration fields; invalid YAML/TOML
is replaced by a placeholder instead of echoing potentially sensitive input.

Redaction applies to the annotated fields and error fields in DM's configured
loggers. It does not scrub arbitrary application text or rewrite existing log
files. Treat logs collected before enabling redaction as potentially sensitive.

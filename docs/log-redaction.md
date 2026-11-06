# TiCDC log redaction

TiCDC can hide sensitive values in annotated log fields or mark them for later
redaction. Redaction is disabled by default.

Set the mode in the TiCDC server configuration:

```toml
[security]
redact-info-log = true
```

The supported configuration values are:

| Value | Behavior | Example field value |
| --- | --- | --- |
| `false` | Keep the original value | `customer-data` |
| `true` | Replace the value | `?` |
| `"MARKER"` | Enclose the original value in markers | `‹customer-data›` |

You can also set `cdc server --redact-info-log=true` or
`cdc server --redact-info-log=MARKER`. An explicitly supplied command-line flag
overrides the configuration file, including `--redact-info-log=false`.
Restart the server to change its configured mode.

MARKER mode retains sensitive data in the original log. Remove the marked content
before sharing that log. Literal `‹` and `›` characters within a sensitive value
are doubled so they can be distinguished from the enclosing markers.

## Coverage

The annotated fields cover row values, keys and key spans, DML arguments,
scheduler messages carrying spans, HTTP request query strings, and
etcd values and watch events that can contain changefeed configuration.
Etcd keys, changefeed identities, timestamps, and other diagnostic fields remain
available. With redaction disabled, the original field values are preserved.

Parameterized DML templates remain readable, including schema, table, and column
names. Their arguments are redacted separately. SQL containing literal row values
must be annotated as sensitive in its entirety.

The configured mode also controls existing TiDB error and TiKV client redaction.
TiKV's key helpers hide keys in both ON and MARKER modes.

DDL text and region error objects remain readable. DDL can contain sensitive
literals, such as a column's `DEFAULT` value. This feature redacts the
annotated fields; it does not guarantee that every log message, error string, or
third-party log is free of sensitive content. DM retains its existing redaction
setting. This facility is not enabled in sync-diff-inspector.

## Annotating sensitive fields

Use the helpers in `pkg/logutil` when adding a field containing sensitive data:

```go
log.Debug("exec row",
    zap.String("sql", queryTemplate),
    logutil.ZapRedactAny("args", args))
```

`ZapRedactString`, `ZapRedactAny`, `ZapRedactStringer`, and `ZapRedactStrings`
preserve the original zap field behavior in OFF mode and replace the content
with `?` in ON mode. MARKER formatting is deferred until zap encodes the field,
so a filtered debug entry does not format its arguments. Each field retains the
redaction decision made when it was constructed.

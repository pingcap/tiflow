# Log Redaction in TiFlow

## Overview

TiFlow supports log redaction to protect sensitive information in log files. This feature helps prevent the accidental exposure of sensitive data such as database connection strings, table data, and other confidential information in logs.

## Redaction Modes

TiFlow supports three redaction modes:

1. **OFF** (default): Disable redaction, all content is logged as-is
2. **ON**: Replace sensitive content with `?`
3. **MARKER**: Surround sensitive content with `‹›` markers for later de-redaction

## Configuration

### TiCDC Server Configuration

For TiCDC server, configure log redaction in the configuration file:

```toml
[log]
# Set redaction mode (true/false/"MARKER")
redact-info-log = true
# Or use MARKER mode for de-redaction
# redact-info-log = "MARKER"
```

Or use command-line flags:

```bash
tiflow server \
  --redact-info-log=true \
  --pd=http://127.0.0.1:2379
```

### DM Worker Configuration  

For DM worker, configure log redaction:

```toml
# Set redaction mode (true/false/"MARKER")
redact-info-log = true
# Or use MARKER mode for de-redaction
# redact-info-log = "MARKER"
```

Or use command-line flags:

```bash
dm-worker \
  --config=worker.toml \
  --redact-info-log=true
```

## Usage Examples

### Example Configuration File

```toml
# tiflow_server.toml
addr = "127.0.0.1:8300"
log-level = "info"
log-file = "tiflow.log"

[log]
redact-info-log = "MARKER"
```

### Programmatic Usage

When developing TiFlow components, use the redaction utilities:

```go
import "github.com/pingcap/tiflow/pkg/logutil"

// Redact sensitive strings
sensitiveData := "user_secret_123"
log.Info("Processing data", zap.String("data", logutil.RedactString(sensitiveData)))

// Redact database keys
key := []byte("sensitive_key") 
log.Info("Key operation", zap.String("key", logutil.RedactKey(key)))

// Use Zap field helpers
log.Info("Database query", 
    logutil.ZapRedactString("query", "SELECT * FROM users WHERE id = 123"),
    logutil.ZapRedactString("table", "users"))
```

## Redaction Examples

### OFF Mode (Default)
```
[INFO] Processing user data: "john_doe@example.com"
[INFO] Query: "SELECT * FROM users WHERE id = 12345"
[INFO] Connection string: "mysql://user:pass@localhost:3306/db"
```

### ON Mode  
```
[INFO] Processing user data: "?"
[INFO] Query: "?"
[INFO] Connection string: "?"
```

### MARKER Mode
```
[INFO] Processing user data: "‹john_doe@example.com›"
[INFO] Query: "‹SELECT * FROM users WHERE id = 12345›"
[INFO] Connection string: "‹mysql://user:pass@localhost:3306/db›"
```

## De-redaction

When using MARKER mode, you can de-redact logs using the redaction utility:

```go
import "github.com/pingcap/tiflow/pkg/logutil/redact"

// De-redact file and remove markers
err := redact.DeRedactFile(false, "redacted.log", "unredacted.log")

// De-redact file and replace marked content with "?"
err := redact.DeRedactFile(true, "redacted.log", "sanitized.log")
```

## Security Considerations

1. **Sensitive Data Identification**: The redaction system only redacts content that is explicitly marked for redaction by the application code. Developers must identify and mark sensitive data appropriately.

2. **MARKER Mode Risks**: When using MARKER mode, sensitive information is still present in logs. Ensure proper access controls and consider using ON mode in production environments.

3. **Configuration Security**: Protect configuration files containing redaction settings, especially in production environments.

4. **Log Rotation**: Ensure log rotation policies account for both redacted and non-redacted log files.

## Unified Configuration

Both TiCDC and DM now use the same `redact-info-log` configuration approach:

- `redact-info-log = false` (default): No redaction
- `redact-info-log = true`: Replace sensitive data with `?`  
- `redact-info-log = "MARKER"`: Surround sensitive data with `‹›` markers

This unified approach provides:

- Consistent redaction behavior across TiCDC, DM, and Engine components
- Multiple redaction modes including de-redaction support  
- Thread-safe global redaction state management
- Simple boolean or string-based configuration

## Best Practices

1. **Production Settings**: Use `redact-info-log = true` in production to ensure sensitive data is completely hidden
2. **Development Settings**: Use `redact-info-log = "MARKER"` in development for easier debugging with de-redaction capability
3. **Testing**: Use `redact-info-log = false` in test environments where full log visibility is needed
4. **Monitoring**: Monitor redaction effectiveness by reviewing log samples
5. **Documentation**: Document what data is considered sensitive in your deployment

## Troubleshooting

### Common Issues

1. **Invalid redaction mode error**: Ensure `redact-info-log` is `true`, `false`, or `"MARKER"` (case-sensitive)
2. **Redaction not working**: Verify the application code uses redaction utilities for sensitive data
3. **Performance impact**: Redaction has minimal performance impact, but MARKER mode uses slightly more memory

### Debugging

To verify redaction is working:

1. Set `redact-info-log = "MARKER"`
2. Check logs contain `‹›` markers around sensitive content  
3. Use de-redaction utility to verify content can be recovered (MARKER mode only)

## API Reference

### Package: `github.com/pingcap/tiflow/pkg/logutil/redact`

Key functions:
- `SetRedactionMode(mode RedactionMode)`: Set global redaction mode
- `String(input string) string`: Redact a string
- `Key(key []byte) string`: Redact a byte key  
- `ZapString(key, value string) zap.Field`: Create redacted zap field

### Package: `github.com/pingcap/tiflow/pkg/logutil`

Convenience functions:
- `InitLogRedactionFromType(redactType RedactInfoLogType) error`: Initialize redaction from unified type
- `RedactString(s string) string`: Redact string with current settings
- `ZapRedactString(key, value string) zap.Field`: Create redacted zap field
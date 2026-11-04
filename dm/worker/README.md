# DM-worker log redaction

DM enables log redaction by default. Sensitive values passed through DM's
redaction helpers are replaced with `?`. This applies when `redact-info-log` is
omitted from both the worker configuration file and the command line.

To disable redaction for diagnostic logs, explicitly set the worker option:

```toml
redact-info-log = false
```

Alternatively, pass `--redact-info-log=false` to `dm-worker`. Command-line options
override the configuration file, including an explicit `false`. Use
`redact-info-log = true` or `--redact-info-log=true` to enable redaction explicitly.

Deployments upgrading from the previous default of `false` will start redacting
these values unless they explicitly disable the setting. Existing explicit
settings retain their behavior. The option controls the existing redaction
helpers; it does not redact every log field.

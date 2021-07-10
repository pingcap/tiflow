package context

import "context"

var defaultContext context.Context

// SetDefaultContext sets the default context for command line usage.
func SetDefaultContext(ctx context.Context) {
	defaultContext = ctx
}

// GetDefaultContext returns the default context for command line usage.
func GetDefaultContext() context.Context {
	return defaultContext
}

package manager

// ExecutorInfoProvider describes an object that maintains a list
// of all executors
type ExecutorInfoProvider interface {
	HasExecutor(executorID string) bool
	ListExecutors() []string
}

package registry

var globalWorkerRegistry = NewRegistry()

// GlobalWorkerRegistry returns the global worker registry
func GlobalWorkerRegistry() Registry {
	return globalWorkerRegistry
}

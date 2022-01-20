package registry

var globalWorkerRegistry = NewRegistry()

func GlobalWorkerRegistry() Registry {
	return globalWorkerRegistry
}

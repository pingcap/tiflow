package externalresource

// not thread-safe now
type manager struct {
	// resourceID -> executorID
	resourceMap map[string]string
}

var DefaultManager = &manager{
	resourceMap: make(map[string]string),
}

func (m *manager) AddResources(executorID string, resourceIDs []string) {
	for _, rID := range resourceIDs {
		m.resourceMap[rID] = executorID
	}
}

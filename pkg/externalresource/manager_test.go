package externalresource

import "testing"

func TestManager(t *testing.T) {
	m := &manager{
		resourceMap: map[string]string{},
	}
	m.AddResources("eID", []string{"rID1", "rID2"})
}

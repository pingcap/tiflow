package uuid

import (
	guuid "github.com/google/uuid"
)

// Generator defines an interface that can generate a uuid
type Generator interface {
	NewString() string
}

type generatorImpl struct{}

func (g *generatorImpl) NewString() string {
	return guuid.New().String()
}

// NewGenerator creates a new generatorImpl instance
func NewGenerator() Generator {
	return &generatorImpl{}
}

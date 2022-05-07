package uuid

import (
	guuid "github.com/google/uuid"
)

type Generator interface {
	NewString() string
}

type generatorImpl struct{}

func (g *generatorImpl) NewString() string {
	return guuid.New().String()
}

func NewGenerator() Generator {
	return &generatorImpl{}
}

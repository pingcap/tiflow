package uuid

import "github.com/pingcap/log"

// MockGenerator is a mocked uuid generator
type MockGenerator struct {
	list []string
}

// NewMock creates a new MockGenerator instance
func NewMock() *MockGenerator {
	return &MockGenerator{}
}

// NewString implements Generator.NewString
func (g *MockGenerator) NewString() (ret string) {
	if len(g.list) == 0 {
		log.L().Panic("Empty uuid list. Please use Push() to add a uuid to the list.")
	}

	ret, g.list = g.list[0], g.list[1:]
	return
}

// Push adds a candidate uuid in FIFO list
func (g *MockGenerator) Push(uuid string) {
	g.list = append(g.list, uuid)
}

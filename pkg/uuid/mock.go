package uuid

import "github.com/pingcap/log"

type MockGenerator struct {
	list []string
}

func NewMock() *MockGenerator {
	return &MockGenerator{}
}

func (g *MockGenerator) NewString() (ret string) {
	if len(g.list) == 0 {
		log.L().Panic("Empty uuid list. Please use Push() to add a uuid to the list.")
	}

	ret, g.list = g.list[0], g.list[1:]
	return
}

func (g *MockGenerator) Push(uuid string) {
	g.list = append(g.list, uuid)
}

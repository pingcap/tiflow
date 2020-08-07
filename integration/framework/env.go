package framework

type MqListener func (states interface{}, topic string, key []byte, value []byte) error

type Environment interface {
	Setup()
	TearDown()
	Reset()
	RunTest(Task)
	SetListener(states interface{}, listener MqListener)
}

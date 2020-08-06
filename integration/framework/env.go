package framework

type MqListener func (states interface{}, topic string, key []byte, value []byte) error

type Environment interface {
	Setup()
	TearDown()
	Reset()
	RunTest(interface{})
	SetListener(states interface{}, listener MqListener)
}

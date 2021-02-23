package orchestrator

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

type ReactorStateTester struct {
	state     ReactorState
	kvEntries map[string]string
}

func (t *ReactorStateTester) UpdateKeys(updatedKeys map[string]string) error {
	for key, value := range updatedKeys {
		k := util.NewEtcdKey(key)
		v := []byte(value)
		err := t.state.Update(k, v, false)
		if err != nil {
			return errors.Trace(err)
		}
		t.kvEntries[key] = value
	}

	return nil
}

func (t *ReactorStateTester) ApplyPatches() error {
	
}

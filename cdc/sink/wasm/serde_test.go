package wasm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalCommonExecResp(t *testing.T) {
	s := []byte(`{"code":200,"message":"success","cause":null}`)
	var resp CommonExecResp
	err := WasmUnmarshal(s, &resp)
	assert.NoError(t, err)
	assert.Equal(t, int32(RespCodeSuccess), resp.Code)
}

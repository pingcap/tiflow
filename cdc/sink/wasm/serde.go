package wasm

import (
	"encoding/json"
	"fmt"
)

const RespCodeSuccess = 200

type RespStatus struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
	Cause   string `json:"cause"`
}

type AddTableReq struct {
	TableID int64 `json:"table_id"`
}

type RemoveTableReq struct {
	TableID int64 `json:"table_id"`
}

type CommonExecResp struct {
	RespStatus
}

func WasmMarshal(s interface{}) ([]byte, error) {
	return json.Marshal(s)
}

// WasmUnmarshal
// Currently we use json for convinience
func WasmUnmarshal(b []byte, s interface{}) error {
	return json.Unmarshal(b, s)
}

func (r RespStatus) Error() string {
	return fmt.Sprintf("code: %d, msg: %s, cause: %s", r.Code, r.Message, r.Cause)
}

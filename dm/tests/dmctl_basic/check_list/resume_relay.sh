#!/bin/bash

: '
TODO
» resume-relay -s 172.17.0.6:8262 -s 172.17.0.2:8262
{
    "op": "InvalidRelayOp",
    "result": true,
    "msg": "",
    "workers": [
        {
            "op": "ResumeRelay",
            "result": true,
            "worker": "172.17.0.2:8262",
            "msg": ""
        },
        {
            "op": "ResumeRelay",
            "result": true,
            "worker": "172.17.0.6:8262",
            "msg": ""
        }
    ]
}
'

function resume_relay_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-relay wrong_arg" \
		"resume-relay <-s source ...> \[flags\]" 1
}

function resume_relay_wihout_worker() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-relay" \
		"must specify at least one source" 1
}

function resume_relay_while_master_down() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-relay -s $SOURCE_ID1 -s $SOURCE_ID2" \
		"can not resume relay unit:" 1
}

function resume_relay_success() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-relay -s $SOURCE_ID1 -s $SOURCE_ID2" \
		"\"result\": true" 3 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1
}

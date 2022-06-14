import sys
import os
import requests as rq
import time
import json

# the max retry time
RETRY_TIME = 10

BASE_URL0 = "https://127.0.0.1:8300/api/v1"
BASE_URL1 = "https://127.0.0.1:8301/api/v1"

V2_BASE_URL0 = "https://127.0.0.1:8300/api/v2"

# we should write some SQLs in the run.sh after call create_changefeed
def create_changefeed(sink_uri):
    url = BASE_URL1+"/changefeeds"
    # create changefeed
    for i in range(1, 4):
        data = {
            "changefeed_id": "changefeed-test"+str(i),
            "sink_uri": "blackhole://",
            "ignore_ineligible_table": True
        }
        # set sink_uri
        if i == 1 and sink_uri != "":
            data["sink_uri"] = sink_uri

        data = json.dumps(data)
        headers = {"Content-Type": "application/json"}
        resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
        assert resp.status_code == rq.codes.accepted

    # create changefeed fail because sink_uri is invalid
    data = json.dumps({
        "changefeed_id": "changefeed-test",
        "sink_uri": "mysql://127.0.0.1:1111",
        "ignore_ineligible_table": True
    })
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request

    print("pass test: create changefeed")


def list_changefeed():
    # test state: all
    url = BASE_URL0+"/changefeeds?state=all"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    # test state: normal
    url = BASE_URL0+"/changefeeds?state=normal"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok
    data = resp.json()
    for changefeed in data:
        assert changefeed["state"] == "normal"

    # test state: stopped
    url = BASE_URL0+"/changefeeds?state=stopped"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok
    data = resp.json()
    for changefeed in data:
        assert changefeed["state"] == "stopped"

    print("pass test: list changefeed")

def get_changefeed():
    # test get changefeed success
    url = BASE_URL0+"/changefeeds/changefeed-test1"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    # test get changefeed failed
    url = BASE_URL0+"/changefeeds/changefeed-not-exists"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request
    data = resp.json()
    assert data["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: get changefeed")


def pause_changefeed():
    # pause changefeed
    url = BASE_URL0+"/changefeeds/changefeed-test2/pause"
    for i in range(RETRY_TIME):
        resp = rq.post(url, cert=CERT, verify=VERIFY)
        if resp.status_code == rq.codes.accepted:
            break
        time.sleep(1)
    assert resp.status_code == rq.codes.accepted
    # check if pause changefeed success
    url = BASE_URL0+"/changefeeds/changefeed-test2"
    for i in range(RETRY_TIME):
        resp = rq.get(url, cert=CERT, verify=VERIFY)
        assert resp.status_code == rq.codes.ok
        data = resp.json()
        if data["state"] == "stopped":
            break
        time.sleep(1)
    assert data["state"] == "stopped"
    # test pause changefeed failed
    url = BASE_URL0+"/changefeeds/changefeed-not-exists/pause"
    resp = rq.post(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request
    data = resp.json()
    assert data["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: pause changefeed")

def update_changefeed():
    # update fail
    # can only update a stopped changefeed
    url = BASE_URL0+"/changefeeds/changefeed-test1"
    data = json.dumps({"mounter_worker_num": 32})
    headers = {"Content-Type": "application/json"}
    resp = rq.put(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request

    # update success
    url = BASE_URL0+"/changefeeds/changefeed-test2"
    data = json.dumps({"mounter_worker_num": 32})
    headers = {"Content-Type": "application/json"}
    resp = rq.put(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.accepted

    # update fail
    # can't update start_ts
    url = BASE_URL0+"/changefeeds/changefeed-test2"
    data = json.dumps({"start_ts": 0})
    headers = {"Content-Type": "application/json"}
    resp = rq.put(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request

    print("pass test: update changefeed")


def resume_changefeed():
    # resume changefeed
    url = BASE_URL1+"/changefeeds/changefeed-test2/resume"
    resp = rq.post(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.accepted

    # check if resume changefeed success
    url = BASE_URL1+"/changefeeds/changefeed-test2"
    for i in range(RETRY_TIME):
        resp = rq.get(url, cert=CERT, verify=VERIFY)
        assert resp.status_code == rq.codes.ok
        data = resp.json()
        if data["state"] == "normal":
            break
        time.sleep(1)
    assert data["state"] == "normal"

    # test resume changefeed failed
    url = BASE_URL0+"/changefeeds/changefeed-not-exists/resume"
    resp = rq.post(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request
    data = resp.json()
    assert data["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: resume changefeed")


def remove_changefeed():
    # remove changefeed
    url = BASE_URL0+"/changefeeds/changefeed-test3"
    resp = rq.delete(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.accepted

    # check if remove changefeed success
    url = BASE_URL0+"/changefeeds/changefeed-test3"
    for i in range(RETRY_TIME):
        resp = rq.get(url, cert=CERT, verify=VERIFY)
        if resp.status_code == rq.codes.bad_request:
            break
        time.sleep(1)
    assert resp.status_code == rq.codes.bad_request
    assert resp.json()["error_code"] == "CDC:ErrChangeFeedNotExists"

    # test remove changefeed failed
    url = BASE_URL0+"/changefeeds/changefeed-not-exists"
    resp = rq.delete(url, cert=CERT, verify=VERIFY)
    assert (resp.status_code == rq.codes.bad_request or resp.status_code == rq.codes.internal_server_error)
    data = resp.json()
    assert data["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: remove changefeed")


def rebalance_table():
    # rebalance_table
    url = BASE_URL0 + "/changefeeds/changefeed-test1/tables/rebalance_table"
    resp = rq.post(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.accepted

    print("pass test: rebalance table")


def move_table():
    # move table
    url = BASE_URL0 + "/changefeeds/changefeed-test1/tables/move_table"
    data = json.dumps({"capture_id": "test-aaa-aa", "table_id": 11})
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.accepted

    # move table fail
    # not allow empty capture_id
    data = json.dumps({"capture_id": "", "table_id": 11})
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request

    print("pass test: move table")


def resign_owner():
    url = BASE_URL1 + "/owner/resign"
    resp = rq.post(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.accepted

    print("pass test: resign owner")


def list_capture():
    url = BASE_URL0 + "/captures"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    print("pass test: list captures")


def list_processor():
    url = BASE_URL0 + "/processors"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    print("pass test: list processors")


def get_processor():
    # list processor to get changefeed_id and capture_id 
    base_url = BASE_URL0 + "/processors"
    resp = rq.get(base_url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok
    data = resp.json()[0]
    url = base_url + "/" + data["changefeed_id"] + "/" + data["capture_id"]
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    # print error message for debug 
    if (resp.status_code != rq.codes.ok):
        print("request url", url)
        print("response status code:", resp.status_code)
        print("response body:", resp.text())
    assert resp.status_code == rq.codes.ok

    # test capture_id error and cdc server no panic
    url = base_url + "/" + data["changefeed_id"] + "/" + "non-exist-capture-id"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request

    print("pass test: get processors")


def check_health():
    url = BASE_URL0 + "/health"
    for i in range(RETRY_TIME):
        resp = rq.get(url, cert=CERT, verify=VERIFY)
        if resp.status_code == rq.codes.ok:
            break
        time.sleep(1)
    assert resp.status_code == rq.codes.ok

    print("pass test: check health")


def get_status():
    url = BASE_URL0 + "/status"
    resp = rq.get(url,cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok
    assert resp.json()["is_owner"]

    print("pass test: get status")


def set_log_level():
    url = BASE_URL0 + "/log"
    data = json.dumps({"log_level": "debug"})
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    data = json.dumps({"log_level": "info"})
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    print("pass test: set log level")

def get_tso():
    # test state: all
    url = V2_BASE_URL0+"/tso"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    print("pass test: get tso")

# arg1: test case name
# arg2: cetificates dir
# arg3: sink uri
if __name__ == "__main__":

    CERTIFICATE_PATH = sys.argv[2]
    CLIENT_PEM_PATH = CERTIFICATE_PATH + '/client.pem'
    CLIENT_KEY_PEM_PATH = CERTIFICATE_PATH + '/client-key.pem'
    CA_PEM_PATH = CERTIFICATE_PATH + '/ca.pem'
    CERT=(CLIENT_PEM_PATH, CLIENT_KEY_PEM_PATH)
    VERIFY=(CA_PEM_PATH)

    # test all the case as the order list in this map
    FUNC_MAP = {
        "check_health": check_health,
        "get_status": get_status,
        "create_changefeed": create_changefeed,
        "list_changefeed": list_changefeed,
        "get_changefeed": get_changefeed,
        "pause_changefeed": pause_changefeed,
        "update_changefeed": update_changefeed,
        "resume_changefeed": resume_changefeed,
        "rebalance_table": rebalance_table,
        "move_table": move_table,
        "get_processor": get_processor,
        "list_processor": list_processor,
        "set_log_level": set_log_level,
        "remove_changefeed": remove_changefeed,
        "resign_owner": resign_owner,
        "get_tso": get_tso
    }

    func = FUNC_MAP[sys.argv[1]]
    if len(sys.argv) >= 3:
        func(*sys.argv[3:])
    else:
        func()

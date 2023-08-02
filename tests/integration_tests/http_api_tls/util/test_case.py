import sys
import os
import requests as rq
import time
import json

# the max retry time
RETRY_TIME = 10

BASE_URL0 = "https://127.0.0.1:8300/api/v1"
BASE_URL1 = "https://127.0.0.1:8301/api/v1"

BASE_URL0_V2 = "https://127.0.0.1:8300/api/v2"
BASE_URL1_V2 = "https://127.0.0.1:8301/api/v2"

TLS_PD_ADDR = "https://127.0.0.1:2579"
SINK_URI="mysql://normal:123456@127.0.0.1:3306/"

physicalShiftBits = 18
# we should write some SQLs in the run.sh after call create_changefeed
def create_changefeed(sink_uri):
    url = BASE_URL1+"/changefeeds"
    # create changefeed
    for i in range(1, 5):
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
    time.sleep(2)
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

def verify_table():
    url = BASE_URL0_V2+"/tso"
    # we need to retry since owner resign before this func call
    i = 0
    while i < 10:
        try:
            data = json.dumps({})
            headers = {"Content-Type": "application/json"}
            resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY, timeout=5)
            if resp.status_code == rq.codes.ok:
              break
            else:
                 continue
        except rq.exceptions.RequestException:
            i += 1
    assert resp.status_code == rq.codes.ok

    ps = resp.json()["timestamp"]
    ls = resp.json()["logic_time"]
    tso = compose_tso(ps,ls)

    url = BASE_URL0_V2 + "/verify_table"
    data = json.dumps({
    "pd_addrs": [TLS_PD_ADDR],
    "ca_path":CA_PEM_PATH,
    "cert_path":CLIENT_PEM_PATH,
    "key_path":CLIENT_KEY_PEM_PATH,
    "cert_allowed_cn":["client"],
    "start_ts": tso,
    "replica_config": {
        "filter": {
            "rules": ["test.verify*"]
            }
        }
    })
    headers = {"Content-Type": "application/json"}
    for i in range(RETRY_TIME):
        resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
        if resp.status_code == rq.codes.ok:
            break
        time.sleep(1)
    assert resp.status_code == rq.codes.ok
    eligible_table_name = resp.json()["eligible_tables"][0]["table_name"]
    ineligible_table_name = resp.json()["ineligible_tables"][0]["table_name"]
    assert eligible_table_name == "verify_table_eligible"
    assert ineligible_table_name == "verify_table_ineligible"

    print("pass test: verify table")


def get_tso():
    # test state: all
    url = BASE_URL0_V2+"/tso"
    data = json.dumps({})
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    print("pass test: get tso")

def create_changefeed_v2():
    url = BASE_URL1_V2+"/changefeeds"
    # create changefeed 1
    data = {
        "changefeed_id": "changefeed-test-v2-black-hole-1",
        "sink_uri": "blackhole://",
        "replica_config":{
            "ignore_ineligible_table": True
            },
        "pd_addrs": [TLS_PD_ADDR],
        "ca_path":CA_PEM_PATH,
        "cert_path":CLIENT_PEM_PATH,
        "key_path":CLIENT_KEY_PEM_PATH,
        "cert_allowed_cn":["client"],
    }
    data = json.dumps(data)
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    # create changefeed 2
    data = {
        "changefeed_id": "changefeed-test-v2-black-hole-2",
        "sink_uri": SINK_URI,
        "replica_config":{
            "ignore_ineligible_table": True,
            "filter": {
            "rules": ["test.verify*"]
            }
        },
        "pd_addrs": [TLS_PD_ADDR],
        "ca_path":CA_PEM_PATH,
        "cert_path":CLIENT_PEM_PATH,
        "key_path":CLIENT_KEY_PEM_PATH,
        "cert_allowed_cn":["client"],
    }
    data = json.dumps(data)
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    # create changefeed fail because sink_uri is invalid
    data = json.dumps({
        "changefeed_id": "changefeed-test",
        "sink_uri": "mysql://127.0.0.1:1111",
        "replica_config":{
            "ignore_ineligible_table": True
            },
        "pd_addrs": [TLS_PD_ADDR],
        "ca_path":CA_PEM_PATH,
        "cert_path":CLIENT_PEM_PATH,
        "key_path":CLIENT_KEY_PEM_PATH,
        "cert_allowed_cn":["client"],
    })
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.bad_request

    print("pass test: create changefeed v2")

def unsafe_apis():
    url = BASE_URL1_V2+"/unsafe/metadata"
    resp = rq.get(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok
    print("status code", resp.status_code)
    print("pass test: show metadata")

    # service_gc_safepoint 1
    url = BASE_URL1_V2+"/unsafe/service_gc_safepoint"
    data = {
        "pd_addrs": [TLS_PD_ADDR],
        "ca_path":CA_PEM_PATH,
        "cert_path":CLIENT_PEM_PATH,
        "key_path":CLIENT_KEY_PEM_PATH,
        "cert_allowed_cn":["client"],
    }
    data = json.dumps(data)
    headers = {"Content-Type": "application/json"}
    resp = rq.delete(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    print("status code", resp.status_code)
    assert resp.status_code == rq.codes.ok

    data = json.dumps({})
    headers = {"Content-Type": "application/json"}
    resp = rq.delete(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    print("status code", resp.status_code)
    assert resp.status_code == rq.codes.ok
    print("pass test: delete service_gc_safepoint")

    # create changefeed fail because sink_uri is invalid
    data = json.dumps({})
    url = BASE_URL1_V2+"/unsafe/resolve_lock"
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers, cert=CERT, verify=VERIFY)
    print("status code", resp.status_code)
    assert resp.status_code != rq.codes.not_found
    print("pass test: resolve lock")

def delete_changefeed_v2():
    # remove changefeed
    url = BASE_URL0_V2+"/changefeeds/changefeed-test4"
    resp = rq.delete(url, cert=CERT, verify=VERIFY)
    assert resp.status_code == rq.codes.ok

    # check if remove changefeed success
    url = BASE_URL0+"/changefeeds/changefeed-test4"
    for i in range(RETRY_TIME):
        resp = rq.get(url, cert=CERT, verify=VERIFY)
        if resp.status_code == rq.codes.bad_request:
            break
        time.sleep(1)
    assert resp.status_code == rq.codes.bad_request
    assert resp.json()["error_code"] == "CDC:ErrChangeFeedNotExists"

    # test remove changefeed not exists
    url = BASE_URL0_V2+"/changefeeds/changefeed-not-exists"
    resp = rq.delete(url, cert=CERT, verify=VERIFY)
    assert (resp.status_code == rq.codes.ok)

    print("pass test: remove changefeed v2")

# util functions define belows

# compose physical time and logical time into tso
def compose_tso(ps, ls):
    return (ps << physicalShiftBits) + ls

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
        # api v2
        "get_tso": get_tso,
        "verify_table": verify_table,
        "create_changefeed_v2": create_changefeed_v2,
        "delete_changefeed_v2": delete_changefeed_v2,
        "unsafe_apis": unsafe_apis
    }

    func = FUNC_MAP[sys.argv[1]]
    if len(sys.argv) >= 3:
        func(*sys.argv[3:])
    else:
        func()

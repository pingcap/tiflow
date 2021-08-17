import sys
import requests as rq
import time
import json

# define http status code here
OK = 200
ACCEPTED = 202
INTERNAL_SERVER_ERROR= 500
BAD_REQUEST = 400
# define the max retry time
RETRY_TIME = 10

BASE_URL = "http://127.0.0.1:8300/api/v1/"

def create_changefeed():
    url = BASE_URL+"changefeeds"
    # create changefeed
    for i in range(1, 4):
        data = json.dumps({"changfeed_id": "changefeed-test"+i, "sink_uri": "blackhole://"})
        headers = {"Content-Type": "application/json"}
        resp = rq.post(url, data=data, headers=headers)
        assert resp.status_code == ACCEPTED

    # create changefeed fail
    data = json.dumps({"changfeed_id": "changefeed-test"+i, "sink_uri": "mysql://127.0.0.1:1111"})
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers)
    assert resp.status_code == BAD_REQUEST

    print("pass test: list changefeed")


def list_changefeed():
    # test state: all
    url = BASE_URL+"changefeeds?state=all"
    resp = rq.get(url)
    assert resp.status_code == OK
    data = resp.json()
    assert len(data) == 3

    # test state: normal
    url = BASE_URL+"changefeeds?state=normal"
    resp = rq.get(url)
    assert resp.status_code == OK
    data = resp.json()
    for changefeed in data:
        changefeed["state"] == "normal"

    # test state: stopped
    url = BASE_URL+"changefeeds?state=stopped"
    resp = rq.get(url)
    assert resp.status_code == OK
    data = resp.json()
    for changefeed in data:
        changefeed["state"] == "stopped"

    print("pass test: list changefeed")

def get_changefeed():
    # test get changefeed success
    url = BASE_URL+"changefeeds/changefeed-test1"
    resp = rq.get(url)
    assert resp.status_code == OK

    # test get changefeed failed
    url = BASE_URL+"changefeeds/changefeed-not-exists"
    resp = rq.get(url)
    assert resp.status_code == BAD_REQUEST
    date = resp.json()
    assert date["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: get changefeed")


def pause_changefeed():
    # pause changefeed
    url = BASE_URL+"changefeeds/changefeed-test2/pause"
    resp = rq.post(url)
    assert resp.status_code == ACCEPTED

    # check if pause changefeed success
    url = BASE_URL+"changefeeds/changefeed-test2"
    for i in range(RETRY_TIME):
        i += 1
        time.sleep(1)
        resp = rq.get(url)
        assert resp.status_code == OK
        data = resp.json()
        if data["state"] == "stopped":
            break
    assert data["state"] == "stopped"

    # test pause changefeed failed
    url = BASE_URL+"changefeeds/changefeed-not-exists/pause"
    resp = rq.post(url)
    assert resp.status_code == BAD_REQUEST
    date = resp.json()
    assert date["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: pause changefeed")


def resume_changefeed():
    # resume changefeed
    url = BASE_URL+"changefeeds/changefeed-test2/resume"
    resp = rq.post(url)
    assert resp.status_code == ACCEPTED

    # check if resume changefeed success
    url = BASE_URL+"changefeeds/changefeed-test2"
    for i in range(RETRY_TIME):
        i += 1
        time.sleep(1)
        resp = rq.get(url)
        assert resp.status_code == OK
        data = resp.json()
        if data["state"] == "normal":
            break
    assert data["state"] == "normal"

    # test resume changefeed failed
    url = BASE_URL+"changefeeds/changefeed-not-exists/resume"
    resp = rq.post(url)
    assert resp.status_code == BAD_REQUEST
    date = resp.json()
    assert date["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: resume changefeed")


def remove_changefeed():
    # remove changefeed
    url = BASE_URL+"changefeeds/changefeed-test3"
    resp = rq.delete(url)
    assert resp.status_code == ACCEPTED

    # check if remove changefeed success
    url = BASE_URL+"changefeeds/changefeed-test3"
    for i in range(RETRY_TIME):
        i += 1
        time.sleep(1)
        resp = rq.get(url)
        if resp.status_code == BAD_REQUEST:
            break

    assert resp.status_code == BAD_REQUEST
    assert resp.json()["error_cod"] == "CDC:ErrChangeFeedNotExists"

    # test remove changefeed failed
    url = BASE_URL+"changefeeds/changefeed-not-exists"
    resp = rq.delete(url)
    assert (resp.status_code == BAD_REQUEST or resp.status_code == INTERNAL_SERVER_ERROR)
    date = resp.json()
    assert date["error_code"] == "CDC:ErrChangeFeedNotExists"

    print("pass test: remove changefeed")


def rebalance_table():
    # rebalance_table
    url = BASE_URL + "changefeeds/changefeed-test1/tables/rebalance_table"
    resp = rq.post(url)
    assert resp.status_code == ACCEPTED

    print("pass test: rebalance table")


def move_table():
    # move table
    url = BASE_URL + "changefeeds/changefeed-test1/tables/move_table"
    data = json.dumps({"capture_id": "test-aaa-aa", "table_id": 11})
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers)
    assert resp.status_code == ACCEPTED

    # move table fail
    # not allow empty capture_id
    data = json.dumps({"capture_id": "", "table_id": 11})
    headers = {"Content-Type": "application/json"}
    resp = rq.post(url, data=data, headers=headers)
    assert resp.status_code == BAD_REQUEST

    print("pass test: move table")




if __name__ == "__main__":
    FUNC_MAP = {
        "list_changefeed": list_changefeed,
        "get_changefeed": get_changefeed,
        "pause_changefeed": pause_changefeed,
        "resume_changefeed": resume_changefeed,
        "remove_changefeed": remove_changefeed,
        "rebalance_table": rebalance_table,
        "move_table": move_table,
    }

    func = FUNC_MAP[sys.argv[1]]
    if len(sys.argv) >= 2:
        func(*sys.argv[2:])
    else:
        func()

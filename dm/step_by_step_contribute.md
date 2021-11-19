# Step By Step Contribute Example

This document introduces a step by step example of contriuting to DM.

Please perform the following steps to create your Pull Request to this repository. If don't like to use command, you can also use [GitHub Desktop](https://desktop.github.com/), which is easier to get started.

> **Note:**
>
> This section takes creating a PR to the `master` branch as an example. Steps of creating PRs to other branches are similar.

### Step 1: Fork the repository

1. Visit the project: <https://github.com/pingcap/dm>
2. Click the **Fork** button on the top right and wait it to finish.

### Step 2: Clone the forked repository to local storage

```sh
cd $working_dir # Comes to the directory that you want put the fork in, for example, "cd ~/code"
git clone git@github.com:$user/dm.git # Replace "$user" with your GitHub ID

cd $working_dir/dm
git remote add upstream git@github.com:pingcap/dm.git # Adds the upstream repo
git remote -v # Confirms that your remote makes sense
```

### Step 3: Setting up your development environment

1. Setting up a Go development environment: [How to Write Go Code](http://golang.org/doc/code.html).

    > **Note:**
    >
    > DM uses [`Go Modules`](https://github.com/golang/go/wiki/Modules) to manage dependencies. The version of Go should be **1.16** or above.

    You'll need `GOPATH` defined, and `PATH` modified to access your Go binaries. A
    common setup is the following but you could always google a setup for your own
    flavor.

    ```sh
    export GOPATH=$HOME/go
    export PATH=$PATH:$GOPATH/bin
    ```

    Then you can use `make` command to build DM.

    ```sh
    make build
    ```

2. Setting up test environment. See [Test Preparations](tests/README.md#Preparations) for more details.

### Step 4: Pick up a issue

You can start by finding an existing issue with the
[help wanted](https://github.com/pingcap/dm/labels/help%20wanted)
label in the DM repository. These issues are well suited for new contributors.

> **Note:**
>
> This section takes issue [UCP: retrieve the configuration of the running task from the DM cluster](https://github.com/pingcap/dm/issues/182) as an example. Steps of pick up other issues are similar.

### Step 5: Create a new branch

1. Get your local master up-to-date with upstream/master.

    ```bash
    cd $working_dir/dm
    git fetch upstream
    git checkout master
    git rebase upstream/master
    ```

2. Create a new branch based on the master branch.

    ```bash
    git checkout -b new-branch-name
    ```

### Step 6: Edit your code

Edit some code on the `new-branch-name` branch and save your changes to fix the issure. Below is a example to add `get-task-config` command for DM.

1. Update the [proto code](dm/proto/dmmaster.proto) for grpc

    ```protobuf
    // GetTaskCfg implements a rpc method to get task config
    rpc GetTaskCfg(GetTaskCfgRequest) returns(GetTaskCfgResponse) {
        ...
    }

    // GetTaskCfgRequest is a rpc request for GetTaskCfg
    message GetTaskCfgRequest {
        ...
    }

    // GetTaskCfgRequest is a rpc response for GetTaskCfg
    message GetTaskCfgResponse {
        ...
    }
    ```

2. Generate proto code

    ```sh
    make generate_proto
    ```

3. Generate mock code

    ```sh
    make generate_mock
    ```

4. Add new command for dmctl in [root commnd](dm/ctl/ctl.go)

    ```go
    master.NewGetTaskCfgCmd()
    ```

5. Implement new command for [dmctl](dm/ctl/master/get_task_config.go)

    ```go
    // NewGetTaskCfgCmd creates a getTaskCfg command
    func NewGetTaskCfgCmd() *cobra.Command {
        ...
        cmd := &cobra.Command{
            Run:   getTaskCfgFunc,
        }
        return cmd
    }

    // getTaskCfgFunc does get task's config
    func getTaskCfgFunc(cmd *cobra.Command, _ []string) {
        ...
        cli := common.MasterClient()
        resp, err := cli.GetTaskCfg(ctx, &pb.GetTaskCfgRequest{
            Name: taskName,
        })
        common.PrettyPrintResponse(resp)
    }
    ```

6. Implement new command for [dm-master](dm/master/server.go)

    ```go
    // GetTaskCfg implements MasterServer.GetSubTaskCfg
    func (s *Server) GetTaskCfg(ctx context.Context, req *pb.GetTaskCfgRequest) (*pb.GetTaskCfgResponse, error) {
        ...
        cfg := s.scheduler.GetTaskCfg(req.Name)
        return &pb.GetTaskCfgResponse{
            Result: true,
            Cfg:    cfg,
        }, nil
    }
    ```

7. Add some error instance for your new command in [error_list](pkg/terror/error_list.go)

    ```go
    ErrSchedulerTaskNotExist = New(codeSchedulerTaskNotExist, ClassScheduler, ScopeInternal, LevelMedium, "task with name %s not exist", "Please use `query-status` command to see tasks.")
    ```

8. Generate new [errors.toml](errors.toml)

    ```sh
    make terror_check
    ```

9. Build your code

    ```sh
    make build
    ```

### Step 7: Add Unit Test

1. Add unit test for [dm-master server](dm/master/server_test.go)

    ```go
    func (t *testMaster) TestGetTaskCfg(c *check.C) {
        ...
    }
    ```


2. Run unit test

    ```sh
    make unit_test
    ```

### Step 8: Add Integration Test

1. Add integration test for [dmctl command](tests/dmctl_basic/check_list/get_task_config.sh)

    ```sh
    function get_task_config_to_file() {
        ...
    }
    ```

2. Setup two MySQL server with binlog enabled first and set `GITD_MODE=ON`

    ```sh
    docker run --rm --name mysql-3306 -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 mysql:5.7.22 --log-bin=mysql-bin --port=3306 --bind-address=0.0.0.0 --binlog-format=ROW --server-id=1 --gtid_mode=ON --enforce-gtid-consistency=true > mysql.3306.log 2>&1 &
    docker run --rm --name mysql-3307 -p 3307:3307 -e MYSQL_ROOT_PASSWORD=123456 mysql:5.7.22 --log-bin=mysql-bin --port=3307 --bind-address=0.0.0.0 --binlog-format=ROW --server-id=1 --gtid_mode=ON --enforce-gtid-consistency=true > mysql.3307.log 2>&1 &
    ```

3. Run integration test

    ```sh
    make integration_test
    ```

### Step 9: Commit your changes

```sh
git status # Checks the local status
git add <file> ... # Adds the file(s) you want to commit. If you want to commit all changes, you can directly use `git add .`
git commit -m "commit-message: update the xx"
```

### Step 10: Keep your branch in sync with upstream/master

```sh
# While on your new branch
git fetch upstream
git rebase upstream/master
```

### Step 11: Push your changes to the remote

```sh
git push -u origin new-branch-name # "-u" is used to track the remote branch from origin
```

### Step 12: Create a pull request

1. Visit your fork at <https://github.com/$user/dm> (replace `$user` with your GitHub ID)
2. Click the `Compare & pull request` button next to your `new-branch-name` branch to create your PR.

Now, your PR is successfully submitted! After this PR is merged, you will automatically become a contributor to DM.

> **Note:**
>
> You can find this contribute example in PR [#798](https://github.com/pingcap/dm/pull/798)

## Contact

Join the Slack channel: [#sig-tools](https://tidbcommunity.slack.com/archives/C013HGZMBAR)

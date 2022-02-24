CONCURRENT_NUMBER = 8

def prepare_binaries() {
    stage('Prepare Binaries') {
        def prepares = [:]

        prepares["build binaries"] = {
            node ("${GO_TEST_SLAVE}") {
                container("golang") {
                    println "debug command:\nkubectl -n jenkins-ci exec -ti ${NODE_NAME} bash"
                    def ws = pwd()
                    deleteDir()
                    unstash 'ticdc'

                    dir("go/src/github.com/pingcap/tiflow") {
                        sh """
                            GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make cdc
                            GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make integration_test_build
                            GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make kafka_consumer
                            GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make check_failpoint_ctl
                            tar czvf ticdc_bin.tar.gz bin/*
                            curl -F test/cdc/ci/ticdc_bin_${env.BUILD_NUMBER}.tar.gz=@ticdc_bin.tar.gz http://fileserver.pingcap.net/upload
                        """
                    }
                    dir("go/src/github.com/pingcap/tiflow/tests") {
                        def cases_name = sh (
                            script: 'find . -maxdepth 2 -mindepth 2 -name \'run.sh\' | awk -F/ \'{print $2}\'',
                            returnStdout: true
                        ).trim().split().join(" ")
                        sh "echo ${cases_name} > CASES"
                    }
                    stash includes: "go/src/github.com/pingcap/tiflow/tests/CASES", name: "cases_name", useDefaultExcludes: false
                }
            }
        }
        parallel prepares
    }
}

def tests(sink_type, node_label) {
    stage("Tests") {
        def test_cases = [:]

        test_cases["unit test"] = {
            node (node_label) {
                container("golang") {
                    def ws = pwd()
                    deleteDir()
                    println "debug command:\nkubectl -n jenkins-ci exec -ti ${NODE_NAME} bash"
                    println "work space path:\n${ws}"
                    unstash 'ticdc'

                    dir("go/src/github.com/pingcap/tiflow") {
                        sh """
                            rm -rf /tmp/tidb_cdc_test
                            mkdir -p /tmp/tidb_cdc_test
                            GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make test
                            rm -rf cov_dir
                            mkdir -p cov_dir
                            ls /tmp/tidb_cdc_test
                            cp /tmp/tidb_cdc_test/cov*out cov_dir
                        """
                        sh """
                        tail /tmp/tidb_cdc_test/cov*
                        """
                    }
                    stash includes: "go/src/github.com/pingcap/tiflow/cov_dir/**", name: "unit_test", useDefaultExcludes: false
                }
            }
        }

        def run_integration_test = { step_name, case_names ->
            node (node_label) {
                container("golang") {
                    def ws = pwd()
                    deleteDir()
                    println "debug command:\nkubectl -n jenkins-ci exec -ti ${NODE_NAME} bash"
                    println "work space path:\n${ws}"
                    println "this step will run tests: ${case_names}"
                    unstash 'ticdc'
                    dir("go/src/github.com/pingcap/tiflow") {
                        download_binaries()
                        try {
                            sh """
                                sudo pip install s3cmd
                                rm -rf /tmp/tidb_cdc_test
                                mkdir -p /tmp/tidb_cdc_test
                                echo "${env.KAFKA_VERSION}" > /tmp/tidb_cdc_test/KAFKA_VERSION
                                GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make integration_test_${sink_type} CASE="${case_names}"
                                rm -rf cov_dir
                                mkdir -p cov_dir
                                ls /tmp/tidb_cdc_test
                                cp /tmp/tidb_cdc_test/cov*out cov_dir || touch cov_dir/dummy_file_${step_name}
                            """
                            // cyclic tests do not run on kafka sink, so there is no cov* file.
                            sh """
                            tail /tmp/tidb_cdc_test/cov* || true
                            """
                        } catch (Exception e) {
                            sh """
                                echo "archive all log"
                                for log in `ls /tmp/tidb_cdc_test/*/*.log`; do
                                    dirname=`dirname \$log`
                                    basename=`basename \$log`
                                    mkdir -p "log\$dirname"
                                    tar zcvf "log\${log}.tgz" -C "\$dirname" "\$basename"
                                done
                            """
                            archiveArtifacts artifacts: "log/tmp/tidb_cdc_test/**/*.tgz", caseSensitive: false
                            throw e;
                        }
                    }
                    stash includes: "go/src/github.com/pingcap/tiflow/cov_dir/**", name: "integration_test_${step_name}", useDefaultExcludes: false
                }
            }
        }


        unstash 'cases_name'
        def cases_name = sh (
            script: 'cat go/src/github.com/pingcap/tiflow/tests/CASES',
            returnStdout: true
        ).trim().split()

        def step_cases = []
        def step_length = (int)(cases_name.size() / CONCURRENT_NUMBER + 0.5)
        for(int i in 1..CONCURRENT_NUMBER) {
            def end = i*step_length-1
            if (i == CONCURRENT_NUMBER){
                end = cases_name.size()-1
            }
            step_cases.add(cases_name[(i-1)*step_length..end])
        }
        step_cases.eachWithIndex{ case_names, index ->
            def step_name = "step_${index}"
            test_cases["integration test ${step_name}"] = {
                run_integration_test(step_name, case_names.join(" "))
            }
        }

        parallel test_cases
    }
}

def download_binaries(){
    def TIDB_BRANCH = params.getOrDefault("release_test__tidb_commit", "master")
    def TIKV_BRANCH = params.getOrDefault("release_test__tikv_commit", "master")
    def PD_BRANCH = params.getOrDefault("release_test__pd_commit", "master")
    // TODO master tiflash release version is 4.1.0-rc-43-gxxx, which is not compatible
    // with TiKV 5.0.0-rc.x. Change default branch to master after TiFlash fixes it.
    def TIFLASH_BRANCH = params.getOrDefault("release_test__release_branch", "release-5.0-rc")
    def TIFLASH_COMMIT = params.getOrDefault("release_test__tiflash_commit", null)

    // parse tidb branch
    def m1 = ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m1) {
        TIDB_BRANCH = "${m1[0][1]}"
    }
    m1 = null
    println "TIDB_BRANCH=${TIDB_BRANCH}"

    // parse tikv branch
    def m2 = ghprbCommentBody =~ /tikv\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m2) {
        TIKV_BRANCH = "${m2[0][1]}"
    }
    m2 = null
    println "TIKV_BRANCH=${TIKV_BRANCH}"

    // parse pd branch
    def m3 = ghprbCommentBody =~ /pd\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m3) {
        PD_BRANCH = "${m3[0][1]}"
    }
    m3 = null
    println "PD_BRANCH=${PD_BRANCH}"

    // parse tiflash branch
    def m4 = ghprbCommentBody =~ /tiflash\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m4) {
        TIFLASH_BRANCH = "${m4[0][1]}"
    }
    m4 = null
    println "TIFLASH_BRANCH=${TIFLASH_BRANCH}"

    println "debug command:\nkubectl -n jenkins-ci exec -ti ${NODE_NAME} bash"
    def tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
    def tikv_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
    def pd_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
    def tiflash_sha1 = TIFLASH_COMMIT
    if (TIFLASH_COMMIT == null) {
        tiflash_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tiflash/${TIFLASH_BRANCH}/sha1").trim()
    }
    sh """
        mkdir -p third_bin
        mkdir -p tmp
        mkdir -p bin

        tidb_url="${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz"
        tikv_url="${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz"
        pd_url="${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz"
        tiflash_url="${FILE_SERVER_URL}/download/builds/pingcap/tiflash/${TIFLASH_BRANCH}/${tiflash_sha1}/centos7/tiflash.tar.gz"
        minio_url="${FILE_SERVER_URL}/download/minio.tar.gz"

        curl \${tidb_url} | tar xz -C ./tmp bin/tidb-server
        curl \${pd_url} | tar xz -C ./tmp bin/*
        curl \${tikv_url} | tar xz -C ./tmp bin/tikv-server
        curl \${minio_url} | tar xz -C ./tmp/bin minio
        mv tmp/bin/* third_bin
        curl \${tiflash_url} | tar xz -C third_bin
        mv third_bin/tiflash third_bin/_tiflash
        mv third_bin/_tiflash/* third_bin
        curl ${FILE_SERVER_URL}/download/builds/pingcap/go-ycsb/test-br/go-ycsb -o third_bin/go-ycsb
        curl -L http://fileserver.pingcap.net/download/builds/pingcap/cdc/etcd-v3.4.7-linux-amd64.tar.gz | tar xz -C ./tmp
        mv tmp/etcd-v3.4.7-linux-amd64/etcdctl third_bin
        curl http://fileserver.pingcap.net/download/builds/pingcap/cdc/sync_diff_inspector.tar.gz | tar xz -C ./third_bin
        curl -L https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 -o jq
        mv jq third_bin
        chmod a+x third_bin/*
        rm -rf tmp
        curl -L http://fileserver.pingcap.net/download/test/cdc/ci/ticdc_bin_${env.BUILD_NUMBER}.tar.gz | tar xvz -C .
        mv ./third_bin/* ./bin
        rm -rf third_bin
    """
}

def coverage() {
    stage('Coverage') {
        node("${GO_TEST_SLAVE}") {
            def ws = pwd()
            deleteDir()
            unstash 'ticdc'
            unstash 'unit_test'
            for(int i in 1..CONCURRENT_NUMBER) {
                unstash "integration_test_step_${i-1}"
            }

            dir("go/src/github.com/pingcap/tiflow") {
                container("golang") {
                    archiveArtifacts artifacts: 'cov_dir/*', fingerprint: true

                    timeout(30) {
                        sh """
                        rm -rf /tmp/tidb_cdc_test
                        mkdir -p /tmp/tidb_cdc_test
                        cp cov_dir/* /tmp/tidb_cdc_test
                        set +x
                        BUILD_NUMBER=${env.BUILD_NUMBER} CODECOV_TOKEN="${CODECOV_TOKEN}" COVERALLS_TOKEN="${COVERALLS_TOKEN}" GOPATH=${ws}/go:\$GOPATH PATH=${ws}/go/bin:/go/bin:\$PATH JenkinsCI=1 make coverage
                        set -x
                        """
                    }
                }
            }
        }
    }
}

return this

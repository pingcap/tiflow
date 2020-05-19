def prepare_binaries() {
    stage('Prepare Binaries') {
        def TIDB_BRANCH = "master"
        def TIKV_BRANCH = "master"
        def PD_BRANCH = "master"

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

        def prepares = [:]

        prepares["download third binaries"] = {
            node ("${GO_TEST_SLAVE}") {
                deleteDir()
                container("golang") {
                    def tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
                    def tikv_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
                    def pd_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
                    sh """
                        mkdir -p third_bin
                        mkdir -p tmp

                        tidb_url="${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz"
                        tikv_url="${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz"
                        pd_url="${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz"

                        curl \${tidb_url} | tar xz -C ./tmp bin/tidb-server
                        curl \${pd_url} | tar xz -C ./tmp bin/*
                        curl \${tikv_url} | tar xz -C ./tmp bin/tikv-server
                        mv tmp/bin/* third_bin
                        curl http://download.pingcap.org/tiflash-nightly-linux-amd64.tar.gz | tar xz -C third_bin
                        mv third_bin/tiflash-nightly-linux-amd64/* third_bin
                        curl ${FILE_SERVER_URL}/download/builds/pingcap/go-ycsb/test-br/go-ycsb -o third_bin/go-ycsb
                        curl -L https://github.com/etcd-io/etcd/releases/download/v3.4.7/etcd-v3.4.7-linux-amd64.tar.gz | tar xz -C ./tmp
                        mv tmp/etcd-v3.4.7-linux-amd64/etcdctl third_bin
                        curl https://download.pingcap.org/tidb-tools-v2.1.6-linux-amd64.tar.gz | tar xz -C ./tmp tidb-tools-v2.1.6-linux-amd64/bin/sync_diff_inspector
                        mv tmp/tidb-tools-v2.1.6-linux-amd64/bin/* third_bin
                        chmod a+x third_bin/*
                        rm -rf tmp
                    """

                    stash includes: "third_bin/**", name: "third_binaries"
                }
            }
        }

        prepares["build binaries"] = {
            node ("${GO_TEST_SLAVE}") {
                container("golang") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'ticdc'

                    dir("go/src/github.com/pingcap/ticdc") {
                        sh """
                            GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make cdc
                            GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make integration_test_build
                            GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make kafka_consumer
                            GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make check_failpoint_ctl
                        """
                    }
                    stash includes: "go/src/github.com/pingcap/ticdc/bin/**", name: "ticdc_binaries", useDefaultExcludes: false
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
                    unstash 'ticdc_binaries'

                    dir("go/src/github.com/pingcap/ticdc") {
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
                    stash includes: "go/src/github.com/pingcap/ticdc/cov_dir/**", name: "unit_test", useDefaultExcludes: false
                }
            }
        }

        def run_integration_test = { case_name ->
            node (node_label) {
                container("golang") {
                    def ws = pwd()
                    deleteDir()
                    println "debug command:\nkubectl -n jenkins-ci exec -ti ${NODE_NAME} bash"
                    println "work space path:\n${ws}"
                    unstash 'ticdc'
                    unstash 'third_binaries'
                    unstash 'ticdc_binaries'

                    dir("go/src/github.com/pingcap/ticdc") {
                        sh "mv ${ws}/third_bin/* ./bin/"
                        try {
                            sh """
                                rm -rf /tmp/tidb_cdc_test
                                mkdir -p /tmp/tidb_cdc_test
                                GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make integration_test_${sink_type} CASE=${case_name}
                                rm -rf cov_dir
                                mkdir -p cov_dir
                                ls /tmp/tidb_cdc_test
                                cp /tmp/tidb_cdc_test/cov*out cov_dir || touch cov_dir/dummy_file_${case_name}
                            """
                            // cyclic tests do not run on kafka sink, so there is no cov* file.
                            sh """
                            tail /tmp/tidb_cdc_test/cov* || true
                            """
                        } catch (Exception e) {
                            sh """
                                echo "print all log"
                                for log in `ls /tmp/tidb_cdc_test/*/*.log`; do
                                    echo "____________________________________"
                                    echo "\$log"
                                    cat "\$log"
                                    echo "____________________________________"
                                done
                            """
                            throw e;
                        }
                    }
                    stash includes: "go/src/github.com/pingcap/ticdc/cov_dir/**", name: "integration_test_${case_name}", useDefaultExcludes: false
                }
            }
        }
        find_cases().each{ case_name ->
            test_cases["integration test ${case_name}"] = {
                run_integration_test(case_name)
            }
        }

        parallel test_cases
    }
}

def coverage() {
    stage('Coverage') {
        node("${GO_TEST_SLAVE}") {
            def ws = pwd()
            deleteDir()
            unstash 'ticdc'
            unstash 'unit_test'

            find_cases().each{ case_name ->
                unstash "integration_test_${case_name}"
            }

            dir("go/src/github.com/pingcap/ticdc") {
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

def find_cases() {
    dir("go/src/github.com/pingcap/ticdc/tests") {
        return sh (
            script: 'find . -maxdepth 2 -mindepth 2 -name \'run.sh\' | awk -F/ \'{print $2}\'',
            returnStdout: true
        ).trim().split()
    }
}

return this

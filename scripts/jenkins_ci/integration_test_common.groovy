test_case_names = ["simple", "cdc", "multi_capture", "split_region", "row_format", "tiflash", "availability"]

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
                        curl http://139.219.11.38:8000/5mpBK/tiflash.tar.gz | tar xz -C ./tmp/bin tiflash
                        curl http://139.219.11.38:8000/OHIIL/libtiflash_proxy.tar.gz | tar xz -C ./tmp/bin libtiflash_proxy.so
                        curl http://139.219.11.38:8000/buUKY/flash_cluster_manager.tgz | tar xz && mv flash_cluster_manager ./tmp/bin
                        mv tmp/bin/* third_bin
                        curl ${FILE_SERVER_URL}/download/builds/pingcap/go-ycsb/test-br/go-ycsb -o third_bin/go-ycsb
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
                            sh """
                            tail /tmp/tidb_cdc_test/cov*
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

        test_case_names.each{ case_name ->
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

            test_case_names.each{ case_name ->
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

return this

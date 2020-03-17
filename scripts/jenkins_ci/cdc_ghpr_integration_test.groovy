def test_case_names = ["simple", "cdc", "multi_capture", "split_region", "row_format"]
catchError {
    stage('Prepare Binaries') {
        def prepares = [:]

        prepares["download third binaries"] = {
            container("golang") {
                def ws = pwd()
                deleteDir()

                sh "mkdir -p third_bin"

                sh "mkdir -p tmp"

                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/700d9def026185fe836dd56b0c39e0b4df3c320b/centos7/tidb-server.tar.gz | tar xz -C ./tmp bin/tidb-server"

                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/pd/08d927675c8feb30552f9fb27246b120cc9ed6d7/centos7/pd-server.tar.gz | tar xz -C ./tmp bin/*"

                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tikv/eeaf4be81fabb71c30f62bc9fd11e77860d47d02/centos7/tikv-server.tar.gz | tar xz -C ./tmp bin/tikv-server"

                sh "mv tmp/bin/* third_bin"

                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/go-ycsb/test-br/go-ycsb -o third_bin/go-ycsb"

                sh "curl https://download.pingcap.org/tidb-tools-v2.1.6-linux-amd64.tar.gz | tar xz -C ./tmp tidb-tools-v2.1.6-linux-amd64/bin/sync_diff_inspector"

                sh "mv tmp/tidb-tools-v2.1.6-linux-amd64/bin/* third_bin"

                sh "chmod a+x third_bin/*"

                sh "rm -rf tmp"

                stash includes: "third_bin/**", name: "third_binaries"
            }
        }

        prepares["build binaries"] = {
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

        parallel prepares

    }

    stage("Tests") {
        def tests = [:]

        tests["unit test"] = {
            node ("${GO_TEST_SLAVE}") {
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
            node ("${GO_TEST_SLAVE}") {
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
                                GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make integration_test CASE=${case_name}
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
            tests["integration test ${case_name}"] = {
                run_integration_test(case_name)
            }
        }

        parallel tests
    }

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
    currentBuild.result = "SUCCESS"
}

stage('Summary') {
    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
    def slackmsg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" +
    "${ghprbPullLink}" + "\n" +
    "${ghprbPullDescription}" + "\n" +
    "Unit Test Result: `${currentBuild.result}`" + "\n" +
    "Elapsed Time: `${duration} mins` " + "\n" +
    "${env.RUN_DISPLAY_URL}"

    if (currentBuild.result != "SUCCESS") {
        slackSend channel: '#jenkins-ci', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
    }
}
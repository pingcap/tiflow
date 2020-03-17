catchError {
    stage('Prepare') {
        node ("${GO_TEST_SLAVE}") {
            container("golang") {
                def ws = pwd()
                deleteDir()
                dir("/home/jenkins/agent/git/ticdc") {
                    if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                        deleteDir()
                    }
                    try {
                        checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github-sre-bot-ssh', refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:pingcap/ticdc.git']]]
                    } catch (error) {
                        retry(2) {
                            echo "checkout failed, retry.."
                            sleep 60
                            if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                                deleteDir()
                            }
                            checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github-sre-bot-ssh', refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:pingcap/ticdc.git']]]
                        }
                    }
                }

                dir("go/src/github.com/pingcap/ticdc") {
                    sh """
                        cp -R /home/jenkins/agent/git/ticdc/. ./
                        git checkout -f ${ghprbActualCommit}
                        mkdir -p bin
                        make cdc
                    """
                }

                stash includes: "go/src/github.com/pingcap/ticdc/**", name: "ticdc", useDefaultExcludes: false

                sh "mkdir -p third_bin"

                sh "mkdir -p tmp"

                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/700d9def026185fe836dd56b0c39e0b4df3c320b/centos7/tidb-server.tar.gz | tar xz -C ./tmp bin/tidb-server"

                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/pd/08d927675c8feb30552f9fb27246b120cc9ed6d7/centos7/pd-server.tar.gz | tar xz -C ./tmp bin/*"

                sh "mv tmp/bin/* third_bin"

                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/go-ycsb/test-br/go-ycsb -o third_bin/go-ycsb"

                sh "curl http://fileserver.pingcap.net/download/files/test/cdc/tikv-server -o third_bin/tikv-server"

                sh "curl https://download.pingcap.org/tidb-tools-v2.1.6-linux-amd64.tar.gz | tar xz -C ./tmp tidb-tools-v2.1.6-linux-amd64/bin/sync_diff_inspector"

                sh "mv tmp/tidb-tools-v2.1.6-linux-amd64/bin/* third_bin"

                sh "chmod a+x third_bin/*"

                sh "rm -rf tmp"

                stash includes: "third_bin/**", name: "binaries"
            }
        }
    }

    stage("Tests") {
        def tests = [:]

        tests["unit test"] = {
            node("${GO_TEST_SLAVE}") {
                container("golang") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'ticdc'

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
            node("${GO_TEST_SLAVE}") {
                container("golang") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'ticdc'
                    unstash 'binaries'

                    dir("go/src/github.com/pingcap/ticdc") {
                        sh "mv ${ws}/third_bin/* ./bin/"
                        try {
                            sh """
                                rm -rf /tmp/tidb_cdc_test
                                mkdir -p /tmp/tidb_cdc_test
                                GO111MODULE=off GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make integration_test_build && make integration_test CASE=${case_name}
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

        tests["integration test simple"] = {
            run_integration_test("simple")
        }

        tests["integration test cdc"] = {
            run_integration_test("cdc")
        }

        tests["integration test multi_capture"] = {
            run_integration_test("multi_capture")
        }

        tests["integration test split_region"] = {
            run_integration_test("split_region")
        }

        tests["integration test row_format"] = {
            run_integration_test("row_format")
        }

        parallel tests
    }

    stage('Coverage') {
        node("${GO_TEST_SLAVE}") {
            def ws = pwd()
            deleteDir()
            unstash 'ticdc'
            unstash 'unit_test'
            unstash 'integration_test_simple'
            unstash 'integration_test_cdc'
            unstash 'integration_test_multi_capture'
            unstash 'integration_test_split_region'

            dir("go/src/github.com/pingcap/ticdc") {
                container("golang") {
                    archiveArtifacts artifacts: 'cov_dir/*', fingerprint: true

                    timeout(30) {
                        sh """
                        rm -rf /tmp/tidb_cdc_test
                        mkdir -p /tmp/tidb_cdc_test
                        cp cov_dir/* /tmp/tidb_cdc_test
                        set +x
                        BUILD_NUMBER=${BUILD_NUMBER} CODECOV_TOKEN="${CODECOV_TOKEN}" COVERALLS_TOKEN="${COVERALLS_TOKEN}" GOPATH=${ws}/go:\$GOPATH PATH=${ws}/go/bin:/go/bin:\$PATH JenkinsCI=1 make coverage
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
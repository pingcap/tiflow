def script_path = "go/src/github.com/pingcap/ticdc/scripts/jenkins_ci/integration_test_common.groovy"
println script_path
sh"""
wc -l ${script_path}
"""
def common = load script_path

// HACK! Download jks by injecting RACK_COMMAND
// https://git.io/JJZXX -> https://github.com/pingcap/ticdc/raw/6e62afcfecc4e3965d8818784327d4bf2600d9fa/tests/_certificates/kafka.server.keystore.jks
// https://git.io/JJZXM -> https://github.com/pingcap/ticdc/raw/6e62afcfecc4e3965d8818784327d4bf2600d9fa/tests/_certificates/kafka.server.truststore.jks
def download_jks = 'curl -sfL https://git.io/JJZXX -o /tmp/kafka.server.keystore.jks && curl -sfL https://git.io/JJZXM -o /tmp/kafka.server.truststore.jks'

catchError {
    common.prepare_binaries()

    def label = "cdc-kafka-integration-${UUID.randomUUID().toString()}"
    podTemplate(label: label, idleMinutes: 0,
        containers: [
            containerTemplate(name: 'golang',alwaysPullImage: false, image: "${GO_DOCKER_IMAGE}",
            resourceRequestCpu: '2000m', resourceRequestMemory: '4Gi',
            ttyEnabled: true, command: 'cat'),
            containerTemplate(name: 'zookeeper',alwaysPullImage: false, image: 'wurstmeister/zookeeper',
            resourceRequestCpu: '2000m', resourceRequestMemory: '4Gi',
            ttyEnabled: true),
            containerTemplate(
                name: 'kafka',
                image: 'wurstmeister/kafka',
                resourceRequestCpu: '2000m', resourceRequestMemory: '4Gi',
                ttyEnabled: true,
                alwaysPullImage: false,
                envVars: [
                    envVar(key: 'KAFKA_MESSAGE_MAX_BYTES', value: '1073741824'),
                    envVar(key: 'KAFKA_REPLICA_FETCH_MAX_BYTES', value: '1073741824'),
                    envVar(key: 'KAFKA_BROKER_ID', value: '1'),
                    envVar(key: 'RACK_COMMAND', value: download_jks),
                    envVar(key: 'KAFKA_LISTENERS', value: 'SSL://127.0.0.1:9093,PLAINTEXT://127.0.0.1:9092'),
                    envVar(key: 'KAFKA_ADVERTISED_LISTENERS', value: 'SSL://127.0.0.1:9093,PLAINTEXT://127.0.0.1:9092'),
                    envVar(key: 'KAFKA_SSL_KEYSTORE_LOCATION', value: '/tmp/kafka.server.keystore.jks'),
                    envVar(key: 'KAFKA_SSL_KEYSTORE_PASSWORD', value: 'test1234'),
                    envVar(key: 'KAFKA_SSL_KEY_PASSWORD', value: 'test1234'),
                    envVar(key: 'KAFKA_SSL_TRUSTSTORE_LOCATION', value: '/tmp/kafka.server.truststore.jks'),
                    envVar(key: 'KAFKA_SSL_TRUSTSTORE_PASSWORD', value: 'test1234'),
                    envVar(key: 'ZK', value: 'zk'),
                    envVar(key: 'KAFKA_ZOOKEEPER_CONNECT', value: 'localhost:2181'),
                ]
        )],
        volumes:[
            emptyDirVolume(mountPath: '/tmp', memory: true),
            emptyDirVolume(mountPath: '/home/jenkins', memory: true)
        ]
    ) {
        common.tests("kafka", label)
    }

    currentBuild.result = "SUCCESS"
}

stage('Summary') {
    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
    def slackmsg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" +
    "${ghprbPullLink}" + "\n" +
    "${ghprbPullDescription}" + "\n" +
    "Integration Kafka Test Result: `${currentBuild.result}`" + "\n" +
    "Elapsed Time: `${duration} mins` " + "\n" +
    "${env.RUN_DISPLAY_URL}"

    if (currentBuild.result != "SUCCESS") {
        slackSend channel: '#jenkins-ci', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
    }
}

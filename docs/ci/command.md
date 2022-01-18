# Commands to trigger [Jenkins](https://ci2.pingcap.net/view/ghpr_cdc/) pipeline

## Guide

1. ci pipeline will be triggered when your comment on pull request matched command.
2. `Only triggered by command`, What does that mean?
   - Yes, this ci will be triggered only when your comment on pr matched command.
   - No, this ci will be triggered by every new commit on current pr, comment matched command also trigger ci pipeline.
3. [Using saved replies will help you trigger these tasks quickly](https://docs.github.com/en/github/writing-on-github/working-with-saved-replies/using-saved-replies)

## Commands

| ci pipeline                                 | Commands                                                    | Only triggered by command |
| ------------------------------------------- | ----------------------------------------------------------- | ------------------------- |
| jenkins-ticdc/verify                        | /run-verify<br />/run-all-tests<br />/merge                 | No                        |
| idc-jenkins-ci/unit-test                    | /run-unit-test<br />/run-all-tests<br />/merge              | No                        |
| idc-jenkins-ci/leak-test                    | /run-leak-test<br />/run-all-tests<br />/merge              | No                        |
| idc-jenkins-ci-ticdc/dm-compatibility-test  | /run-dm-compatibility-test<br />/run-all-tests<br />/merge  | Yes                       |
| idc-jenkins-ci-ticdc/dm-integration-test    | /run-dm-integration-test<br />/run-all-tests<br />/merge    | Yes                       |
| idc-jenkins-ci-ticdc/integration-test       | /run-integration-test<br />/run-all-tests<br />/merge       | Yes                       |
| idc-jenkins-ci-ticdc/kafka-integration-test | /run-kafka-integration-test<br />/run-all-tests<br />/merge | Yes                       |

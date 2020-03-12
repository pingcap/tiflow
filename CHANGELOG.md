# CDC (Change Data Capture) Change Log

All notable changes to this project are documented in this file.

## [4.0.0-beta.2] - 2020-03-13

Initial release of the change data capture, providing following features in this release

- Support capturing change data from TiKV since v4.0.0-beta.2
- Support replicating change data to MySQL protocol compatible database, with a eventual consistency guarantee
- Support replicating change data to Kafka, with either a eventual consistency guarantee or row level order guarantee
- Provide a native support for high availability

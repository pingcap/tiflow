# Support dump files store in S3

## Background

When DM synchronizes data at the full stage, it requires a dynamic space which is related to upstream database instance to dump data files from upstream, and the size of this space will be a factor that has to be considered when customer use resources on the cloud. In order to simplify the effect of this factor, we need support dump files store in S3 which is a dynamic store on the cloud.

### User config change

We still use loader's `dir` to config S3 url, but it should be noted that loader's `import-mode` should not be `loader` and it can be `sql` now, because we use [Lightning](https://github.com/pingcap/tidb/tree/master/br/pkg/lightning) to load files in S3. 
```
loaders:
  global:
    pool-size: 16
    dir: s3://dmbucket/dump?region=us-west-2&endpoint=http://127.0.0.1:8688&access_key=s3accesskey&secret_access_key=s3secretkey&force_path_style=true
    import-mode: sql
```

### Implementation

* Dumpling and Lightning already support S3 and they both import [br storage](https://github.com/pingcap/tidb/tree/master/br/pkg/storage), so DM can can use it and wrap DM's interface in [dm storage](https://github.com/pingcap/tiflow/tree/master/dm/pkg/storage).
* [subtask config](https://github.com/pingcap/tiflow/blob/master/dm/dm/config/subtask.go) needs to adjust S3 path and add `taskName.sourceID` suffix to meet the needs of parallel operations for multiple `subtask`.
* [subtask config](https://github.com/pingcap/tiflow/blob/master/dm/dm/config/subtask.go) needs to check configuration about loader's `dir` and `import-mode` to make sure to use Lightning.
* [lightning loader](https://github.com/pingcap/tiflow/blob/master/dm/loader/lightning.go) needs to support read or delete files in S3 to use dumped data's metadata.
* [lightning loader](https://github.com/pingcap/tiflow/blob/master/dm/loader/lightning.go) needs to configurate Lightning's checkpoint in S3.
* [lightning loader](https://github.com/pingcap/tiflow/blob/master/dm/loader/lightning.go) needs to download Lightning's TLS config to a local path and does not use S3 Path.
* [syncer](https://github.com/pingcap/tiflow/blob/master/dm/syncer/syncer.go) needs to support read or delete files in S3 to use dumped data's metadata.

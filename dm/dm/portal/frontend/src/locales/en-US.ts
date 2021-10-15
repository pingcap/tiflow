export default {
  // start step
  dm_task_config_generator: 'DM Task Configuration Generator',
  new_task_config: 'New Task Configuration',

  // name step
  task_name: 'Task Name',
  sync_mode: 'Sync Mode',
  full_mode: 'Full',
  inc_mode: 'Incremental',
  invalid_task_name: 'Invalid task name',

  // instance step
  port: 'Port:',
  user: 'User:',
  pwd: 'Password:',
  upstream: 'Upstream Instance',
  downstream: 'Downstream Instance',
  upstream_fail: 'Verify upstream instance {sourceId} failed! {errMsg}',
  downstream_fail: 'Verify downstream instance failed! {errMsg}',

  // migrate step
  reset_confirm:
    'Are you sure to reset all operations? It will clear the downstream instance',
  undo_confirm:
    'Are you sure to undo this operation and go back to previous status?',
  new_name: 'Please input the new name：',
  name_can_not_empty: "The name can't be empty",
  name_taken: 'This name has been taken',
  config_create_fail: 'The sync rules are created failed!',
  config_create_ok:
    'The sync rules are created successfully, the file locates in {filepath}',
  back_home_confirm:
    'Are you sure to go back to home page, it will lose all operations!',
  auto_sync: 'Auto sync new added databases and tables from upstream',
  auto_sync_explain:
    "When this option is selected, if the upstream has the new added databases or tables later, they will be sync to the downstream, otherwise they won't.",
  go_back_tooltip: 'Go back',
  reset_tooltip: 'Reset',
  finish_and_download: 'Finish & Download',
  go_home: 'Go Home',

  // modal
  binlog_filter: 'Binlog filter ({target})',
  binlog_modify_warning:
    'Note: It will reset all the tables of this database stayed in upstream filter rules when modify the database filter rules',

  // common
  cancel: 'Cancel',
  pre: 'Pre',
  next: 'Next',
  add: 'Add'
}

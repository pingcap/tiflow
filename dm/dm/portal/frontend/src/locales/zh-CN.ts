export default {
  // start step
  dm_task_config_generator: 'DM 任务配置生成',
  new_task_config: '新建任务配置',

  // name step
  task_name: '任务名称',
  sync_mode: '同步模式',
  full_mode: '全量',
  inc_mode: '增量',
  invalid_task_name: '任务名称不合法',

  // instance step
  port: '端口:',
  user: '用户名:',
  pwd: '密码:',
  upstream: '上游实例',
  downstream: '下游实例',
  upstream_fail: '验证上游实例 {sourceId} 失败！{errMsg}',
  downstream_fail: '验证下游实例失败！{errMsg}',

  // migrate step
  reset_confirm: '你确定要重置所有操作吗？此操作会清空下游实例。',
  undo_confirm: '你确定要撤消此次操作，回到上一步吗？',
  new_name: '请输入新名字：',
  name_can_not_empty: '名字不能为空',
  name_taken: '名字已被占用',
  config_create_fail: '同步规则创建失败！',
  config_create_ok: '同步规则创建成功，文件在服务器上位于 {filepath}',
  back_home_confirm: '你确定要返回首页吗？返回将丢失所有操作！',
  auto_sync: '自动同步上游新增库和新增表',
  auto_sync_explain:
    '当选中此选项时，后续如果上游有新增的库或表，也会同步到下游。否则，后续上游新增的库或表不会同步到下游。',
  go_back_tooltip: '回到上一步',
  reset_tooltip: '重置所有操作',
  finish_and_download: '完成并下载',
  go_home: '返回首页',

  // modal
  binlog_filter: 'Binlog 过滤 ({target})',
  binlog_modify_warning:
    '注意：修改库的过滤规则会重置所有此库的上游表的过滤规则',

  // common
  cancel: '取消',
  pre: '上一步',
  next: '下一步',
  add: '添加'
}

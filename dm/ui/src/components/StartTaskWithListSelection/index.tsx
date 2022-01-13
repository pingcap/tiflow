import React, { useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { Table, TableColumnsType, Button, Space, Modal, message } from '~/uikit'
import { useDmapiGetTaskConfigListQuery } from '~/models/taskConfig'
import {
  Task,
  useDmapiGetTaskListQuery,
  useDmapiStartTaskMutation,
} from '~/models/task'

const StartTaskWithListSelection: React.FC<{
  onCancel: () => void
}> = ({ onCancel }) => {
  const [t] = useTranslation()
  const { data: tasks, isFetching: isFetchingTask } = useDmapiGetTaskListQuery({
    withStatus: false,
  })
  const { data: taskConfigs, isFetching } = useDmapiGetTaskConfigListQuery()
  const [selectedTasks, setSelectedTasks] = useState<Task[]>([])
  const [startTask] = useDmapiStartTaskMutation()

  const selectableTaskConfigs = useMemo(() => {
    const taskNames = new Set(tasks?.data?.map(task => task.name) || [])
    return (
      taskConfigs?.data?.filter(
        taskConfig => !taskNames.has(taskConfig.name)
      ) || []
    )
  }, [taskConfigs, tasks])

  const handleStartTask = async (removeMeta: boolean) => {
    if (selectedTasks.length === 0) {
      Modal.error({
        title: t('error'),
        content: t('please select at least one task'),
      })
      return
    }
    const key = 'startTask-' + Date.now()
    message.loading({ content: t('requesting'), key })
    await Promise.all(
      selectedTasks.map(task => {
        return startTask({ task, remove_meta: removeMeta }).unwrap()
      })
    )
    message.success({ content: t('request success'), key })
  }

  const columns: TableColumnsType<Task> = [
    {
      title: t('task name'),
      dataIndex: 'name',
    },
    {
      title: t('type'),
      dataIndex: 'task_mode',
    },
    {
      title: t('source info'),
      dataIndex: 'source_config',
      render(sourceConfig) {
        return sourceConfig.source_conf?.length > 0
          ? t('{{val}} and {{count}} others', {
              val: `${sourceConfig.source_conf[0].source_name}`,
              count: sourceConfig.source_conf.length,
            })
          : '-'
      },
    },
    {
      title: t('target info'),
      dataIndex: 'target_config',
      render(targetConfig) {
        return `${targetConfig.host}:${targetConfig.port}`
      },
    },
  ]

  const rowSelection = {
    selectedRowKeys: selectedTasks.map(i => i.name),
    onChange: (selectedRowKeys: React.Key[], selectedRows: Task[]) => {
      setSelectedTasks(selectedRows)
    },
  }

  return (
    <div>
      <Table
        className="p-4"
        dataSource={selectableTaskConfigs}
        columns={columns}
        loading={isFetching || isFetchingTask}
        rowKey="name"
        rowSelection={rowSelection}
        pagination={{
          total: selectableTaskConfigs.length,
        }}
      />

      <div className="flex justify-center">
        <Space>
          <Button onClick={onCancel}>{t('cancel')}</Button>
          <Button onClick={() => handleStartTask(true)}>
            {t('start task and remove meta data')}
          </Button>
          <Button type="primary" onClick={() => handleStartTask(false)}>
            {t('start task')}
          </Button>
        </Space>
      </div>
    </div>
  )
}

export default StartTaskWithListSelection

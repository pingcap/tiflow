import React, { useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useDebounce } from 'ahooks'

import i18n from '~/i18n'
import {
  Row,
  Col,
  Button,
  Space,
  Table,
  Input,
  Select,
  Breadcrumb,
  TableColumnsType,
} from '~/uikit'
import {
  Task,
  TaskMigrateTarget,
  useDmapiGetTaskListQuery,
  useDmapiGetTaskMigrateTargetsQuery,
} from '~/models/task'
import { useDmapiGetSourceQuery } from '~/models/source'

const ReplicationDetail: React.FC = () => {
  const [t] = useTranslation()
  const [currentTask, setCurrentTask] = useState<Task>()
  const [currentSourceName, setCurrentSourceName] = useState<string>()
  const [dbPattern, setDbPattern] = useState('')
  const [tablePattern, setTablePattern] = useState('')
  const debouncedDbPattern = useDebounce(dbPattern, { wait: 500 })
  const debouncedTablePattern = useDebounce(tablePattern, { wait: 500 })

  const currentTaskName = currentTask?.name ?? ''
  const { data: taskList, isFetching: isFetchingTaskList } =
    useDmapiGetTaskListQuery({
      withStatus: true,
    })
  const {
    data: migrateTagetData,
    refetch,
    isFetching: isFetchingMigrateTarget,
  } = useDmapiGetTaskMigrateTargetsQuery(
    {
      taskName: currentTaskName,
      sourceName: currentSourceName ?? '',
      schemaPattern: debouncedDbPattern,
      tablePattern: debouncedTablePattern,
    },
    { skip: !currentTask || !currentSourceName }
  )
  const { data: sourceData } = useDmapiGetSourceQuery(
    { sourceName: currentSourceName ?? '' },
    { skip: !currentSourceName }
  )
  const loading = isFetchingTaskList || isFetchingMigrateTarget

  const dataSource = migrateTagetData?.data

  const columns: TableColumnsType<TaskMigrateTarget> = [
    {
      title: t('task name'),
      key: 'taskname',
      render() {
        return currentTask?.name
      },
    },
    {
      title: t('source info'),
      key: 'sourceinfo',
      render() {
        if (sourceData) {
          return `${sourceData?.host}:${sourceData.port}`
        }
        return '-'
      },
    },
    {
      title: t('source schema'),
      dataIndex: 'source_schema',
    },
    {
      title: t('source table'),
      dataIndex: 'source_table',
    },
    {
      title: t('target info'),
      render() {
        return `${currentTask?.target_config.host}:${currentTask?.target_config.port}`
      },
    },
    {
      title: t('target schema'),
      dataIndex: 'target_schema',
    },
    {
      title: t('target table'),
      dataIndex: 'target_table',
    },
  ]

  useEffect(() => {
    if (!currentTask && taskList && taskList.data[0]) {
      setCurrentTask(taskList.data[0])
      setCurrentSourceName(
        taskList.data[0]?.source_config.source_conf?.[0]?.source_name ?? ''
      )
    }
  }, [currentTask, taskList])

  return (
    <div>
      <div className="px-4 pt-4">
        <Breadcrumb>
          <Breadcrumb.Item>{t('migration')}</Breadcrumb.Item>
          <Breadcrumb.Item>{t('replication detail')}</Breadcrumb.Item>
        </Breadcrumb>
      </div>

      <Row className="p-4" justify="space-between">
        <Col span={22}>
          <Space>
            <Select
              placeholder="Select a task"
              value={currentTask?.name}
              className="min-w-100px"
              loading={loading}
              options={taskList?.data?.map(i => ({
                label: i.name,
                value: i.name,
              }))}
            />

            <Select
              placeholder="Select a source"
              value={currentSourceName}
              className="min-w-100px"
              loading={loading}
              options={currentTask?.source_config.source_conf?.map(i => ({
                label: i.source_name,
                value: i.source_name,
              }))}
            />

            <Input
              addonBefore="database"
              value={dbPattern}
              placeholder="source database"
              onChange={e => setDbPattern(e.target.value)}
            />
            <Input
              addonBefore="table"
              value={tablePattern}
              placeholder="source table"
              onChange={e => setTablePattern(e.target.value)}
            />
            <Button onClick={refetch}>{t('check')}</Button>
          </Space>
        </Col>
      </Row>

      <Table
        className="p-4"
        dataSource={dataSource}
        columns={columns}
        loading={loading}
        rowKey="key"
        pagination={{
          total: taskList?.total,
        }}
      />
    </div>
  )
}

export const meta = {
  title: () => i18n.t('replication detail'),
  index: 3,
}

export default ReplicationDetail

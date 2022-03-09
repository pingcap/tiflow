import React, { useEffect, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'

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
import { useFuseSearch } from '~/utils/search'

const ReplicationDetail: React.FC = () => {
  const [t] = useTranslation()
  const [currentTask, setCurrentTask] = useState<Task>()
  const [currentSourceName, setCurrentSourceName] = useState<string>()
  const [dbName, setDbName] = useState('')
  const [tableName, setTableName] = useState('')
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
    },
    { skip: !currentTask || !currentSourceName }
  )
  const { data: sourceData } = useDmapiGetSourceQuery(
    { sourceName: currentSourceName ?? '' },
    { skip: !currentSourceName }
  )
  const loading = isFetchingTaskList || isFetchingMigrateTarget

  const dataSource = useMemo(() => {
    return (
      migrateTagetData?.data?.map((i, index) => ({
        ...i,
        key: index,
        tag: i.source_schema + ' ' + i.source_table, // for fuse search
      })) ?? []
    )
  }, [migrateTagetData])

  const { result, setKeyword } = useFuseSearch(dataSource, {
    keys: ['tag'],
  })

  const columns: TableColumnsType<TaskMigrateTarget & { key: number }> = [
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
    if (dbName || tableName) {
      setKeyword(dbName + ' ' + tableName)
    }
  }, [dbName, tableName])

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
              value={dbName}
              placeholder="source database"
              onChange={e => setDbName(e.target.value)}
            />
            <Input
              addonBefore="table"
              value={tableName}
              placeholder="source table"
              onChange={e => setTableName(e.target.value)}
            />
            <Button onClick={refetch}>{t('check')}</Button>
          </Space>
        </Col>
      </Row>

      <Table
        className="p-4"
        dataSource={result}
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

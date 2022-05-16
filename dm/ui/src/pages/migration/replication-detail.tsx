import React, { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { isEqual } from 'lodash-es'

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

interface ReplicationDetailSearchOptions {
  currentTask?: Task
  currentSourceName: string
  dbPattern: string
  tablePattern: string
}

const ReplicationDetail: React.FC = () => {
  const [t] = useTranslation()

  const [
    { currentTask, currentSourceName, dbPattern, tablePattern },
    setSearchOptions,
  ] = useState<ReplicationDetailSearchOptions>({
    currentTask: undefined,
    currentSourceName: '',
    dbPattern: '',
    tablePattern: '',
  })

  const [payload, setPayload] = useState({
    taskName: currentTask?.name ?? '',
    sourceName: currentSourceName ?? '',
    schemaPattern: dbPattern,
    tablePattern: tablePattern,
  })

  const { data: taskList, isFetching: isFetchingTaskList } =
    useDmapiGetTaskListQuery({
      withStatus: true,
    })
  const {
    data: migrateTagetData,
    refetch,
    isFetching: isFetchingMigrateTarget,
  } = useDmapiGetTaskMigrateTargetsQuery(payload, {
    skip: !payload.taskName || !payload.sourceName,
  })
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
              className="w-150px"
              loading={loading}
              options={taskList?.data?.map(i => ({
                label: i.name,
                value: i.name,
              }))}
              onSelect={(value: string) => {
                const t = taskList?.data.find(i => i.name === value)
                if (t) {
                  setSearchOptions(prev => ({ ...prev, currentTask: t }))
                }
              }}
            />

            <Select
              placeholder="Select a source"
              className="w-150px"
              loading={loading}
              options={currentTask?.source_config.source_conf?.map(i => ({
                label: i.source_name,
                value: i.source_name,
              }))}
              onSelect={(value: string) => {
                setSearchOptions(prev => ({
                  ...prev,
                  currentSourceName: value,
                }))
              }}
            />

            <Input
              addonBefore="database"
              placeholder="source database"
              onChange={e => {
                setSearchOptions(prev => ({
                  ...prev,
                  dbPattern: e.target.value,
                }))
              }}
            />
            <Input
              addonBefore="table"
              placeholder="source table"
              onChange={e => {
                setSearchOptions(prev => ({
                  ...prev,
                  tablePattern: e.target.value,
                }))
              }}
            />
            <Button
              onClick={() => {
                const next = {
                  taskName: currentTask?.name ?? '',
                  sourceName: currentSourceName ?? '',
                  schemaPattern: dbPattern,
                  tablePattern: tablePattern,
                }

                if (isEqual(next, payload)) {
                  refetch()
                } else {
                  setPayload(next)
                }
              }}
            >
              {t('check')}
            </Button>
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

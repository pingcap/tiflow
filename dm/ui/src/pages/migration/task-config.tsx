import React, { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useNavigate, useLocation } from 'react-router-dom'

import {
  Input,
  Table,
  Row,
  Col,
  Space,
  Button,
  Dropdown,
  Menu,
  Modal,
  message,
  TableColumnsType,
  Breadcrumb,
} from '~/uikit'
import {
  RedoOutlined,
  PlusSquareOutlined,
  ExportOutlined,
  ImportOutlined,
  DownOutlined,
  SearchOutlined,
  PlayCircleOutlined,
  ExclamationCircleOutlined,
} from '~/uikit/icons'
import i18n from '~/i18n'
import {
  useDmapiDeleteTaskConfigMutation,
  useDmapiGetTaskConfigListQuery,
} from '~/models/taskConfig'
import { Task, useDmapiStartTaskMutation } from '~/models/task'
import { unimplemented } from '~/utils/unimplemented'
import { useFuseSearch } from '~/utils/search'
import CreateTaskConfig from '~/components/CreateTaskConfig'
import BatchImportTaskConfig from '~/components/BatchImportTaskConfig'

const TaskConfig: React.FC = () => {
  const [t] = useTranslation()
  const [isImportTaskModalVisible, setIsImportTaskModalVisible] =
    useState(false)
  const [selected, setSelected] = useState<Task[]>([])

  const navigate = useNavigate()
  const loc = useLocation()

  const { data, isFetching, refetch } = useDmapiGetTaskConfigListQuery(
    undefined,
    { skip: loc.hash === '#new' || loc.hash === '#edit' }
  )
  const [startTask] = useDmapiStartTaskMutation()
  const [deleteTask] = useDmapiDeleteTaskConfigMutation()

  const handleDeleteTask = (taskName: string) => {
    Modal.confirm({
      title: t('are you sure to delete task {{name}}', { name: taskName }),
      icon: <ExclamationCircleOutlined />,
      onOk: async () => {
        const key = 'deleteTask-' + Date.now()
        message.loading({ content: t('requesting'), key })
        await deleteTask({ taskName }).unwrap()
        message.success({ content: t('request success'), key })
      },
    })
  }

  const rowSelection = {
    selectedRowKeys: selected.map(i => i.name),
    onChange: (selectedRowKeys: React.Key[], selectedRows: Task[]) => {
      setSelected(selectedRows)
    },
  }

  const dataSource = data?.data
  const { result, setKeyword } = useFuseSearch(dataSource, {
    keys: ['name'],
  })
  const columns: TableColumnsType<Task> = [
    {
      title: t('name'),
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
    {
      title: t('migrate rules'),
      render(data: Task) {
        return data.table_migrate_rule.length
      },
    },
    {
      title: t('event filter'),
      render(data: Task) {
        return Object.keys(data.binlog_filter_rule || {}).length
      },
    },
    {
      title: t('operations'),
      render(data) {
        return (
          <Space>
            <Button onClick={() => navigate(`#edit-${data.name}`)} type="link">
              {t('edit')}
            </Button>
            <Button
              onClick={() => handleDeleteTask(data.name)}
              type="link"
              danger
            >
              {t('delete')}
            </Button>
          </Space>
        )
      },
    },
  ]

  const handleStartTask = async (removeMeta: boolean) => {
    if (selected.length === 0) {
      Modal.error({
        title: t('error'),
        content: t('please select at least one task'),
      })
      return
    }
    const key = 'startTask-' + Date.now()
    message.loading({ content: t('requesting'), key })
    await Promise.all(
      selected.map(task => {
        return startTask({ task, remove_meta: removeMeta }).unwrap()
      })
    )
    message.success({ content: t('request success'), key })
  }

  if (loc.hash === '#new' || loc.hash.startsWith('#edit')) {
    return <CreateTaskConfig />
  }

  return (
    <div>
      <div className="px-4 pt-4">
        <Breadcrumb>
          <Breadcrumb.Item>{t('migration')}</Breadcrumb.Item>
          <Breadcrumb.Item>{t('task config')}</Breadcrumb.Item>
        </Breadcrumb>
      </div>

      <Row className="p-4" justify="space-between">
        <Col span={22}>
          <Space>
            <Input
              onChange={e => setKeyword(e.target.value)}
              suffix={<SearchOutlined />}
              placeholder={t('search placeholder')}
            />

            <Button icon={<RedoOutlined />} onClick={refetch}>
              {t('refresh')}
            </Button>

            <Dropdown
              overlay={
                <Menu>
                  <Menu.Item
                    icon={<ExportOutlined />}
                    onClick={unimplemented}
                    key="1"
                  >
                    {t('export')}
                  </Menu.Item>
                </Menu>
              }
            >
              <Button
                onClick={() => setIsImportTaskModalVisible(true)}
                icon={<ImportOutlined />}
              >
                {t('import')} <DownOutlined />
              </Button>
            </Dropdown>

            <Dropdown
              overlay={
                <Menu>
                  <Menu.Item onClick={() => handleStartTask(true)} key="1">
                    {t('start task and remove meta data')}
                  </Menu.Item>
                </Menu>
              }
            >
              <Button
                icon={<PlayCircleOutlined />}
                onClick={() => handleStartTask(false)}
              >
                {t('start task')} <DownOutlined />
              </Button>
            </Dropdown>
          </Space>
        </Col>

        <Col span={2}>
          <Button
            onClick={() => navigate('#new')}
            icon={<PlusSquareOutlined />}
          >
            {t('add')}
          </Button>
        </Col>
      </Row>

      <Table
        className="p-4"
        dataSource={result}
        columns={columns}
        loading={isFetching}
        rowKey="name"
        rowSelection={rowSelection}
        pagination={{
          total: data?.total,
          onChange: () => {
            setSelected([])
          },
        }}
      />

      <Modal
        title={t('import task config')}
        visible={isImportTaskModalVisible}
        footer={null}
        destroyOnClose
        onCancel={() => setIsImportTaskModalVisible(false)}
      >
        <BatchImportTaskConfig />
      </Modal>
    </div>
  )
}

export const meta = {
  title: () => i18n.t('task config'),
  index: 2,
}

export default TaskConfig

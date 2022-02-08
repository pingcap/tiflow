import React, { useCallback, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { PrismAsyncLight as SyntaxHighlighter } from 'react-syntax-highlighter'
import { ghcolors } from 'react-syntax-highlighter/dist/esm/styles/prism'
import json from 'react-syntax-highlighter/dist/esm/languages/prism/json'

import {
  Table,
  TableColumnsType,
  Row,
  Col,
  Space,
  Input,
  Button,
  Dropdown,
  Menu,
  Modal,
  message,
  Drawer,
  Spin,
  Collapse,
  Pagination,
} from '~/uikit'
import {
  SearchOutlined,
  RedoOutlined,
  DownOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  CloseCircleOutlined,
  ExclamationCircleOutlined,
  DatabaseOutlined,
  FlagOutlined,
} from '~/uikit/icons'
import {
  Task,
  useDmapiGetTaskListQuery,
  useDmapiDeleteTaskMutation,
  useDmapiPauseTaskMutation,
  useDmapiResumeTaskMutation,
  useDmapiGetTaskStatusQuery,
  calculateTaskStatus,
} from '~/models/task'
import i18n from '~/i18n'
import { useFuseSearch } from '~/utils/search'
import StartTaskWithListSelection from '~/components/StartTaskWithListSelection'

SyntaxHighlighter.registerLanguage('json', json)

const TaskList: React.FC = () => {
  const [t] = useTranslation()
  const [visible, setVisible] = useState(false)
  const [currentTaskName, setCurrentTaskName] = useState('')
  const [selectedSources, setSelectedSources] = useState<string[]>([])
  const [isStartTaskModalVisible, setIsStartTaskModalVisible] = useState(false)
  const [displayedSubtaskOffset, setDisplayedSubtaskOffset] = useState({
    start: 0,
    end: 10,
  })

  const { data, isFetching, refetch } = useDmapiGetTaskListQuery({
    withStatus: true,
  })
  const { data: currentTaskStatus } = useDmapiGetTaskStatusQuery(
    { taskName: currentTaskName },
    { skip: !currentTaskName }
  )

  const [pauseTask] = useDmapiPauseTaskMutation()
  const [resumeTask] = useDmapiResumeTaskMutation()
  const [deleteTask] = useDmapiDeleteTaskMutation()

  const rowSelection = {
    selectedRowKeys: selectedSources,
    onChange: (selectedRowKeys: React.Key[], selectedRows: Task[]) => {
      setSelectedSources(selectedRows.map(i => i.name))
    },
  }

  const handleRequest = useCallback(
    ({ key, handler, title }) => {
      Modal.confirm({
        title,
        icon: <ExclamationCircleOutlined />,
        onOk() {
          message.loading({ content: t('requesting'), key })
          Promise.all(
            selectedSources.map(name => handler({ taskName: name }).unwrap())
          )
            .then(() => message.success({ content: t('request success'), key }))
            .catch(() => {
              message.destroy(key)
            })
        },
      })
    },
    [selectedSources]
  )

  const handlePauseTask = useCallback(() => {
    if (!selectedSources.length) {
      return
    }
    handleRequest({
      title: t('confirm to pause task?'),
      key: 'pauseTask-' + Date.now(),
      handler: pauseTask,
    })
  }, [selectedSources, handleRequest])
  const handleResumeTask = useCallback(() => {
    if (!selectedSources.length) {
      return
    }
    handleRequest({
      title: t('confirm to resume task?'),
      key: 'resumeTask-' + Date.now(),
      handler: resumeTask,
    })
  }, [selectedSources, handleRequest])
  const handleDeleteTask = useCallback(() => {
    if (!selectedSources.length) {
      return
    }
    handleRequest({
      title: t('confirm to delete task?'),
      key: 'deleteTask-' + Date.now(),
      handler: deleteTask,
    })
  }, [selectedSources, handleRequest])
  const handlePageChange = useCallback((page: number, pageSize: number) => {
    const start = (page - 1) * pageSize
    setDisplayedSubtaskOffset({ start, end: start + pageSize })
  }, [])

  const dataSource = data?.data

  const { result, setKeyword } = useFuseSearch(dataSource, {
    keys: ['name'],
  })

  const columns: TableColumnsType<Task> = [
    {
      title: t('task name'),
      dataIndex: 'name',
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
      title: t('status'),
      dataIndex: 'status_list',
      render(subtasks) {
        return calculateTaskStatus(subtasks)
      },
    },
    {
      title: t('operations'),
      dataIndex: 'name',
      render(name: string) {
        return (
          <Space>
            <Button
              type="link"
              onClick={() => {
                setCurrentTaskName(name)
                setVisible(true)
              }}
            >
              {t('view')}
            </Button>
          </Space>
        )
      },
    },
  ]

  return (
    <div>
      <Row className="p-4" justify="space-between">
        <Col span={22}>
          <Space>
            <Input
              suffix={<SearchOutlined />}
              onChange={e => setKeyword(e.target.value)}
              placeholder={t('search placeholder')}
            />

            <Button icon={<RedoOutlined />} onClick={refetch}>
              {t('refresh')}
            </Button>
            <Dropdown
              overlay={
                <Menu>
                  <Menu.Item
                    icon={<PlayCircleOutlined />}
                    onClick={handleResumeTask}
                  >
                    {t('resume')}
                  </Menu.Item>
                  <Menu.Item
                    icon={<CloseCircleOutlined />}
                    onClick={handleDeleteTask}
                  >
                    {t('delete')}
                  </Menu.Item>
                </Menu>
              }
            >
              <Button onClick={handlePauseTask}>
                <PauseCircleOutlined />
                {t('pause')} <DownOutlined />
              </Button>
            </Dropdown>

            <Button
              icon={<PlayCircleOutlined />}
              onClick={() => setIsStartTaskModalVisible(true)}
            >
              {t('start task')}
            </Button>
          </Space>
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
        }}
      />

      <Drawer
        title={t('task detail')}
        placement="left"
        size="large"
        visible={visible}
        onClose={() => setVisible(false)}
      >
        {currentTaskStatus ? (
          <>
            <Collapse>
              {currentTaskStatus.data
                .slice(displayedSubtaskOffset.start, displayedSubtaskOffset.end)
                .map(item => {
                  return (
                    <Collapse.Panel
                      key={item.name}
                      header={
                        <div className="flex-1 flex justify-between">
                          <span>
                            <DatabaseOutlined className="mr-2" />
                            {item.source_name}
                          </span>
                          <span>
                            <FlagOutlined className="mr-2" />
                            {item.stage}
                          </span>
                        </div>
                      }
                    >
                      <SyntaxHighlighter style={ghcolors} language="json">
                        {JSON.stringify(item, null, 2)}
                      </SyntaxHighlighter>
                    </Collapse.Panel>
                  )
                })}
            </Collapse>

            <div className="flex mt-4 justify-end">
              <Pagination
                onChange={handlePageChange}
                total={currentTaskStatus.total}
              />
            </div>
          </>
        ) : (
          <div className="flex items-center justify-center">
            <Spin />
          </div>
        )}
      </Drawer>

      <Modal
        width={720}
        title={t('start task')}
        visible={isStartTaskModalVisible}
        footer={null}
        destroyOnClose
        onCancel={() => setIsStartTaskModalVisible(false)}
      >
        <StartTaskWithListSelection
          onCancel={() => setIsStartTaskModalVisible(false)}
        />
      </Modal>
    </div>
  )
}

export const meta = {
  title: () => i18n.t('task list'),
  index: 0,
}

export default TaskList

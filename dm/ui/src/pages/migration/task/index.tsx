import React, { useCallback, useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
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
  Breadcrumb,
  Tabs,
  Radio,
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
  PlusSquareOutlined,
} from '~/uikit/icons'
import {
  Task,
  useDmapiGetTaskListQuery,
  useDmapiDeleteTaskMutation,
  useDmapiStopTaskMutation,
  useDmapiStartTaskMutation,
  useDmapiGetTaskStatusQuery,
  calculateTaskStatus,
  TaskStage,
} from '~/models/task'
import i18n from '~/i18n'
import { useFuseSearch } from '~/utils/search'
import { actions, useAppDispatch, useAppSelector } from '~/models'

SyntaxHighlighter.registerLanguage('json', json)

enum CreateTaskMethod {
  ByGuide,
  ByConfigFile,
}

const TaskList: React.FC = () => {
  const [t] = useTranslation()
  const dispatch = useAppDispatch()
  const [visible, setVisible] = useState(false)
  const [currentTask, setCurrentTask] = useState<Task>()
  const [selectedSources, setSelectedSources] = useState<string[]>([])
  const [displayedSubtaskOffset, setDisplayedSubtaskOffset] = useState({
    start: 0,
    end: 10,
  })
  const [isModalVisible, setIsModalVisible] = useState(false)
  const [method, setMethod] = useState(CreateTaskMethod.ByGuide)
  const selectedTask = useAppSelector(state => state.globals.preloadedTask)
  const navigate = useNavigate()

  const { data, isFetching, refetch } = useDmapiGetTaskListQuery({
    withStatus: true,
  })
  const { data: currentTaskStatus } = useDmapiGetTaskStatusQuery(
    { taskName: currentTask?.name ?? '' },
    { skip: !currentTask }
  )

  const [stopTask] = useDmapiStopTaskMutation()
  const [startTask] = useDmapiStartTaskMutation()
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
            selectedSources.map(name => handler({ taskName: name }))
          ).then(res => {
            if (res.some(r => r?.error)) {
              message.destroy(key)
            } else {
              message.success({ content: t('request success'), key })
            }
          })
        },
      })
    },
    [selectedSources]
  )

  const handleStopTask = useCallback(() => {
    if (!selectedSources.length) {
      return
    }
    handleRequest({
      title: t('confirm to stop task?'),
      key: 'stopTask-' + Date.now(),
      handler: stopTask,
    })
  }, [selectedSources, handleRequest])
  const handleStartTask = useCallback(() => {
    if (!selectedSources.length) {
      return
    }
    handleRequest({
      title: t('confirm to start task?'),
      key: 'startTask-' + Date.now(),
      handler: startTask,
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
      render(data) {
        return (
          <Button
            type="link"
            onClick={() => {
              setCurrentTask(data)
              setVisible(true)
            }}
          >
            {data.name}
          </Button>
        )
      },
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
      filters: Object.values(TaskStage).map(stage => ({
        text: stage,
        value: stage,
      })),
      onFilter: (value, record) =>
        calculateTaskStatus(record.status_list) === value,
    },
    {
      title: t('operations'),
      render(data) {
        return (
          <Space>
            <Button
              type="link"
              onClick={() => {
                dispatch(actions.setPreloadedTask(data))
                setIsModalVisible(true)
              }}
            >
              {t('edit')}
            </Button>
          </Space>
        )
      },
    },
  ]

  useEffect(() => {
    dispatch(actions.setPreloadedTask(null))
  }, [])

  return (
    <div>
      <div className="p-4">
        <Breadcrumb>
          <Breadcrumb.Item>{t('migration')}</Breadcrumb.Item>
          <Breadcrumb.Item>{t('task list')}</Breadcrumb.Item>
        </Breadcrumb>
      </div>

      <div className="mx-4 my-2 p-4 rounded bg-white border-1 border-gray-300 border-dashed whitespace-pre-line">
        {t('task list desc')}
      </div>

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
                    icon={<PauseCircleOutlined />}
                    key="stop"
                    onClick={handleStopTask}
                  >
                    {t('stop')}
                  </Menu.Item>
                  <Menu.Item
                    icon={<CloseCircleOutlined />}
                    key="delete"
                    onClick={handleDeleteTask}
                  >
                    {t('delete')}
                  </Menu.Item>
                </Menu>
              }
            >
              <Button onClick={handleStartTask}>
                <PlayCircleOutlined />
                {t('start')} <DownOutlined />
              </Button>
            </Dropdown>
          </Space>
        </Col>
        <Col span={2}>
          <Button
            onClick={() => {
              setIsModalVisible(true)
            }}
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
        }}
      />

      <Drawer
        title={t('task detail')}
        placement="right"
        size="large"
        visible={visible}
        onClose={() => setVisible(false)}
      >
        {currentTaskStatus ? (
          <Tabs defaultActiveKey="1">
            <Tabs.TabPane tab={t('subtask')} key="1">
              <>
                <Collapse>
                  {currentTaskStatus.data
                    .slice(
                      displayedSubtaskOffset.start,
                      displayedSubtaskOffset.end
                    )
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
            </Tabs.TabPane>
            <Tabs.TabPane tab={t('runtime config')} key="2">
              <SyntaxHighlighter style={ghcolors} language="json">
                {JSON.stringify(currentTask, null, 2)}
              </SyntaxHighlighter>
            </Tabs.TabPane>
          </Tabs>
        ) : (
          <div className="flex items-center justify-center">
            <Spin />
          </div>
        )}
      </Drawer>

      <Modal
        onOk={() => {
          if (method === CreateTaskMethod.ByGuide) {
            selectedTask
              ? navigate('/migration/task/edit')
              : navigate('/migration/task/create')
          } else if (method === CreateTaskMethod.ByConfigFile) {
            selectedTask
              ? navigate('/migration/task/edit#configFile')
              : navigate('/migration/task/create#configFile')
          }
          setIsModalVisible(false)
        }}
        onCancel={() => setIsModalVisible(false)}
        okText={t('confirm')}
        cancelText={t('cancel')}
        visible={isModalVisible}
      >
        <div>
          <Radio.Group
            onChange={e => {
              setMethod(e.target.value)
            }}
            defaultValue={method}
          >
            <Space direction="vertical">
              <Radio value={CreateTaskMethod.ByGuide}>
                <div>
                  <div className="font-bold">{t('open task by guide')}</div>
                  <div className="text-gray-400">
                    {t('open task by guide desc')}
                  </div>
                </div>
              </Radio>
              <Radio value={CreateTaskMethod.ByConfigFile}>
                <div>
                  <div className="font-bold">{t('open task by config')}</div>
                  <div className="text-gray-400">
                    {t('open task by config desc')}
                  </div>
                </div>
              </Radio>
            </Space>
          </Radio.Group>
        </div>
      </Modal>
    </div>
  )
}

export const meta = {
  title: () => i18n.t('task list'),
  index: 0,
}

export default TaskList

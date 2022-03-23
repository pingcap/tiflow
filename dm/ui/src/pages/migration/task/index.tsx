import React, { useCallback, useEffect, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import { PrismAsyncLight as SyntaxHighlighter } from 'react-syntax-highlighter'
import { ghcolors } from 'react-syntax-highlighter/dist/esm/styles/prism'
import yaml from 'react-syntax-highlighter/dist/esm/languages/prism/yaml'

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
  Select,
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
  DeploymentUnitOutlined,
  ThunderboltOutlined,
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
  TaskUnit,
  SubTaskStatus,
  useDmapiConverterTaskMutation,
} from '~/models/task'
import i18n from '~/i18n'
import { useFuseSearch } from '~/utils/search'
import { actions, useAppDispatch, useAppSelector } from '~/models'
import { Source, useDmapiGetSourceListQuery } from '~/models/source'
import TaskUnitTag from '~/components/SimpleTaskPanel/TaskUnitTag'

SyntaxHighlighter.registerLanguage('yaml', yaml)

enum CreateTaskMethod {
  ByGuide,
  ByConfigFile,
}

const SourceTable: React.FC<{
  data: Source[]
}> = ({ data }) => {
  const [t] = useTranslation()
  const { result, setKeyword } = useFuseSearch(data, { keys: ['source_name'] })
  const columns: TableColumnsType<Source> = [
    {
      title: t('name'),
      dataIndex: 'source_name',
    },
    {
      title: t('host'),
      render(s: Source) {
        return `${s.host}:${s.port}`
      },
    },
    {
      title: t('user name'),
      dataIndex: 'user',
    },
  ]
  return (
    <div>
      <Input
        className="mb-4"
        suffix={<SearchOutlined />}
        onChange={e => setKeyword(e.target.value)}
        placeholder={t('search placeholder')}
      />
      <Table dataSource={result} columns={columns} rowKey="source_name" />
    </div>
  )
}

const SubTaskTable: React.FC<{ subs: SubTaskStatus[] }> = ({ subs }) => {
  const [stage, setStage] = useState<TaskStage | ''>('')
  const [unit, setUnit] = useState<TaskUnit | ''>('')
  const [page, setPage] = useState(1)
  const offset = useMemo(() => {
    return {
      start: (page - 1) * 10,
      end: page * 10,
    }
  }, [page])
  const data = useMemo(() => {
    let data = subs
    if (stage) {
      data = data.filter(s => s.stage === stage)
    }
    if (unit) {
      data = data.filter(s => s.unit === unit)
    }
    return data
  }, [subs, stage, unit, offset])

  const handlePageChange = (page: number) => {
    setPage(page)
  }

  useEffect(() => {
    // reset offset when filters changed
    setPage(1)
  }, [stage, unit, subs])

  return (
    <>
      <div className="mb-4">
        <Space>
          <Select
            className="min-w-120px"
            placeholder="Stage"
            value={stage}
            onChange={setStage}
          >
            {Object.values(TaskStage).map(stage => (
              <Select.Option key={stage} value={stage}>
                {stage}
              </Select.Option>
            ))}
            <Select.Option key="all" value="">
              All
            </Select.Option>
          </Select>
          <Select
            className="min-w-100px"
            placeholder="Unit"
            value={unit}
            onChange={setUnit}
          >
            {Object.values(TaskUnit).map(i => (
              <Select.Option key={i} value={i}>
                {i}
              </Select.Option>
            ))}
            <Select.Option key="all" value="">
              All
            </Select.Option>
          </Select>
        </Space>
      </div>
      <Collapse>
        {data.slice(offset.start, offset.end).map(item => {
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
                    <DeploymentUnitOutlined className="mr-2" />
                    {item.unit}
                  </span>
                  <span>
                    <FlagOutlined className="mr-2" />
                    {item.stage}
                  </span>
                  <span>
                    <ThunderboltOutlined className="mr-2" />
                    {item.sync_status?.seconds_behind_master ?? 0}s
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
          current={page}
          pageSize={10}
          onChange={handlePageChange}
          total={data.length}
        />
      </div>
    </>
  )
}

const TaskList: React.FC = () => {
  const [t] = useTranslation()
  const dispatch = useAppDispatch()
  const [detailDrawerVisible, setDetailDrawerVisible] = useState(false)
  const [sourceDrawerVisible, setSourceDrawerVisible] = useState(false)
  const [currentTaskName, setCurrentTaskName] = useState<string>()
  const [selectedSources, setSelectedSources] = useState<string[]>([])
  const [isModalVisible, setIsModalVisible] = useState(false)
  const [method, setMethod] = useState(CreateTaskMethod.ByGuide)
  const [currentTaskConfigFile, setCurrentTaskConfigFile] = useState('')
  const selectedTask = useAppSelector(state => state.globals.preloadedTask)
  const navigate = useNavigate()

  const { data, isFetching, refetch } = useDmapiGetTaskListQuery({
    withStatus: true,
  })
  const { data: currentTaskStatus, refetch: refetchCurrentTaskStatus } =
    useDmapiGetTaskStatusQuery(
      { taskName: currentTaskName ?? '' },
      { skip: !currentTaskName }
    )
  const taskMap = useMemo(() => {
    return data?.data?.reduce((acc, cur) => {
      acc.set(cur.name, cur)
      return acc
    }, new Map() as Map<string, Task>)
  }, [data])
  const { data: sources } = useDmapiGetSourceListQuery({ with_status: false })
  const sourcesOfCurrentTask: Source[] = useMemo(() => {
    if (!currentTaskName || !sources) {
      return []
    }
    const currentTask = taskMap?.get(currentTaskName)
    if (currentTask) {
      const names = new Set(
        currentTask.source_config.source_conf.map(i => i.source_name)
      )
      return sources.data.filter(i => names.has(i.source_name))
    }
    return []
  }, [currentTaskName, sources, taskMap])

  const [convertTaskDataToConfigFile] = useDmapiConverterTaskMutation()

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
              setCurrentTaskName(data.name)
              setDetailDrawerVisible(true)
            }}
          >
            {data.name}
          </Button>
        )
      },
    },
    {
      title: t('mode'),
      dataIndex: 'task_mode',
    },
    {
      title: t('source info'),
      render(data) {
        const sourceConfig = data.source_config
        const sources =
          sourceConfig.source_conf?.length > 0
            ? t('{{val}} and {{count}} others', {
                val: `${sourceConfig.source_conf[0].source_name}`,
                count: sourceConfig.source_conf.length,
              })
            : '-'
        if (sources.length > 1) {
          return (
            <Button
              type="link"
              onClick={() => {
                setCurrentTaskName(data.name)
                setSourceDrawerVisible(true)
              }}
            >
              {sources}
            </Button>
          )
        }
        return sources
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
      title: t('current stage'),
      dataIndex: 'status_list',
      render(statusList: Task['status_list']) {
        if (!statusList) return '-'
        return (
          <div>
            <TaskUnitTag status={statusList} />
          </div>
        )
      },
    },
    {
      title: t('status'),
      dataIndex: 'status_list',
      render(subtasks: Task['status_list']) {
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
      title: t('incremental sync delay'),
      dataIndex: 'status_list',
      render(data: Task['status_list']) {
        const syncUnits = data?.filter(i => i.unit === TaskUnit.Sync)
        if (syncUnits && syncUnits.length > 0) {
          return `${Math.max(
            ...syncUnits.map(i => i?.sync_status?.seconds_behind_master ?? 0)
          )}s`
        }
        return '-'
      },
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

  useEffect(() => {
    if (currentTaskName) {
      const currentTask = taskMap?.get(currentTaskName)
      if (currentTask) {
        const { status_list, ...rest } = currentTask
        convertTaskDataToConfigFile({ task: rest })
          .unwrap()
          .then(res => {
            setCurrentTaskConfigFile(res.task_config_file)
          })
      }
    }
  }, [currentTaskName])

  useEffect(() => {
    refetchCurrentTaskStatus()
  }, [data])

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

            <Dropdown.Button
              icon={<DownOutlined />}
              onClick={handleStartTask}
              overlay={
                <Menu>
                  <Menu.Item
                    icon={<PauseCircleOutlined />}
                    key="stop"
                    onClick={handleStopTask}
                  >
                    {t('stop', { context: 'task list' })}
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
              <PlayCircleOutlined />
              {t('start')}
            </Dropdown.Button>
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
        visible={detailDrawerVisible}
        onClose={() => setDetailDrawerVisible(false)}
      >
        {currentTaskStatus ? (
          <Tabs defaultActiveKey="1">
            <Tabs.TabPane tab={t('subtask')} key="1">
              <SubTaskTable subs={currentTaskStatus.data} />
            </Tabs.TabPane>
            <Tabs.TabPane tab={t('runtime config')} key="2">
              <SyntaxHighlighter style={ghcolors} language="yaml">
                {currentTaskConfigFile}
              </SyntaxHighlighter>
            </Tabs.TabPane>
          </Tabs>
        ) : (
          <div className="flex items-center justify-center">
            <Spin />
          </div>
        )}
      </Drawer>

      <Drawer
        title={t('source detail')}
        placement="right"
        size="large"
        visible={sourceDrawerVisible}
        onClose={() => setSourceDrawerVisible(false)}
      >
        <SourceTable data={sourcesOfCurrentTask} />
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

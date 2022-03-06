import React, { useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { PrismAsyncLight as SyntaxHighlighter } from 'react-syntax-highlighter'
import { ghcolors } from 'react-syntax-highlighter/dist/esm/styles/prism'
import json from 'react-syntax-highlighter/dist/esm/languages/prism/json'

import {
  Button,
  Modal,
  Space,
  Input,
  Col,
  Row,
  message,
  Table,
  Badge,
  Breadcrumb,
  Dropdown,
  Menu,
  TableColumnsType,
  Drawer,
  Collapse,
  Spin,
  Tabs,
} from '~/uikit'
import {
  SearchOutlined,
  ExclamationCircleOutlined,
  PlusSquareOutlined,
  DownOutlined,
  PlayCircleOutlined,
  PauseCircleOutlined,
  DeleteOutlined,
  RedoOutlined,
} from '~/uikit/icons'
import CreateOrUpdateSource from '~/components/CreateOrUpdateSource'
import type { Source } from '~/models/source'
import {
  useDmapiCreateSourceMutation,
  useDmapiDeleteSourceMutation,
  useDmapiDisableSourceMutation,
  useDmapiEnableSourceMutation,
  useDmapiGetSourceListQuery,
  useDmapiUpdateSourceMutation,
} from '~/models/source'
import { useDmapiGetTaskListQuery } from '~/models/task'
import i18n from '~/i18n'
import { useFuseSearch } from '~/utils/search'
import { isEmptyObject } from '~/utils/isEmptyObject'
import SimpleTaskPanel from '~/components/SimpleTaskPanel'

SyntaxHighlighter.registerLanguage('json', json)

const SourceList: React.FC = () => {
  const [t] = useTranslation()
  const [showModal, setShowModal] = useState(false)
  const [drawerVisible, setDrawerVisible] = useState(false)
  const [currentSource, setCurrentSource] = useState<Source | null>(null)
  const [selectedSources, setSelectedSources] = useState<string[]>([])

  const { data, isFetching, refetch } = useDmapiGetSourceListQuery({
    with_status: false,
  })
  const { data: taskListData, isFetching: isFetchingTaskListData } =
    useDmapiGetTaskListQuery(
      {
        withStatus: true,
        sourceNameList: currentSource ? [currentSource.source_name] : [],
      },
      { skip: !currentSource }
    )
  const [createSource] = useDmapiCreateSourceMutation()
  const [updateSource] = useDmapiUpdateSourceMutation()
  const [removeSource] = useDmapiDeleteSourceMutation()
  const [disableSource] = useDmapiDisableSourceMutation()
  const [enableSource] = useDmapiEnableSourceMutation()

  const handleAddNew = () => {
    setCurrentSource(null)
    setShowModal(true)
  }
  const handleCancel = () => {
    setShowModal(false)
  }
  const handleConfirm = async (payload: Source) => {
    const isEditing = Boolean(currentSource)
    const key = 'createSource-' + Date.now()
    const emptyKeys = Object.keys(payload).filter(key =>
      isEmptyObject((payload as any)[key])
    )
    emptyKeys.forEach(key => {
      delete (payload as any)[key]
    })
    message.loading({ content: t('requesting'), key })
    const handler = isEditing ? updateSource : createSource
    handler({ source: payload })
      .unwrap()
      .then(() => {
        message.success({ content: t('request success'), key, duration: 6 })
        setShowModal(false)
      })
      .catch(() => {
        message.destroy(key)
      })
  }
  const handleRemoveSource = async () => {
    if (selectedSources.length === 0) return
    const key = 'removeSource-' + Date.now()
    Modal.confirm({
      title: (
        <span>
          {t('confirm to delete source') + ' '}
          <strong>{selectedSources.join(', ')}</strong>?
        </span>
      ),
      icon: <ExclamationCircleOutlined />,
      onOk() {
        message.loading({ content: t('deleting'), key })
        Promise.all(
          selectedSources.map(name => removeSource({ sourceName: name }))
        ).then(() => {
          message.success({ content: t('deleted'), key })
          setSelectedSources([])
        })
      },
    })
  }
  const handleEnableSource = () => {
    if (selectedSources.length === 0) return
    const key = 'enableSource-' + Date.now()
    message.loading({ content: t('requesting'), key })
    Promise.all(selectedSources.map(name => enableSource(name))).then(res => {
      if (res.some(r => (r as any).error)) {
        message.destroy(key)
      } else {
        message.success({ content: t('request success'), key })
      }
    })
  }
  const handleDisableSource = () => {
    if (selectedSources.length === 0) return
    const key = 'disableSource-' + Date.now()
    message.loading({ content: t('requesting'), key })
    Promise.all(selectedSources.map(name => disableSource(name))).then(res => {
      if (res.some(r => (r as any).error)) {
        message.destroy(key)
      } else {
        message.success({ content: t('request success'), key })
      }
    })
  }
  const dataSource = data?.data

  const { result, setKeyword } = useFuseSearch(dataSource, {
    keys: ['source_name', 'host'],
  })

  const columns: TableColumnsType<Source> = [
    {
      title: t('name'),
      render(data) {
        return (
          <Button
            type="link"
            onClick={() => {
              setCurrentSource(data)
              setDrawerVisible(true)
            }}
          >
            {data.source_name}
          </Button>
        )
      },
    },
    {
      title: t('ip'),
      dataIndex: 'host',
    },
    {
      title: t('port'),
      dataIndex: 'port',
    },
    {
      title: t('user name'),
      dataIndex: 'user',
    },
    {
      title: t('is enabled'),
      dataIndex: 'enable',
      render(enabled: boolean) {
        return (
          <Badge
            status={enabled ? 'success' : 'default'}
            text={enabled ? t('enabled') : t('stopped')}
          />
        )
      },
    },
    {
      title: t('relay log'),
      dataIndex: 'relay_config',
      render(relayConfig) {
        return (
          <Badge
            status={relayConfig.enable_relay ? 'success' : 'default'}
            text={relayConfig.enable_relay ? t('on') : t('off')}
          />
        )
      },
    },
    {
      title: t('enable gtid'),
      dataIndex: 'enable_gtid',
      render(enabled: boolean) {
        return (
          <Badge
            status={enabled ? 'success' : 'default'}
            text={enabled ? t('enabled') : t('disabled')}
          />
        )
      },
    },
    {
      title: t('operations'),
      render(data: Source) {
        return (
          <Space>
            <Button
              type="link"
              onClick={() => {
                setCurrentSource(data)
                setShowModal(true)
              }}
            >
              {t('edit')}
            </Button>
          </Space>
        )
      },
    },
  ]
  const rowSelection = {
    selectedRowKeys: selectedSources,
    onChange: (selectedRowKeys: React.Key[], selectedRows: Source[]) => {
      setSelectedSources(selectedRows.map(i => i.source_name))
    },
  }

  useEffect(() => {
    if (!showModal) {
      setCurrentSource(null)
    }
  }, [showModal])

  return (
    <div>
      <div className="px-4 pt-4">
        <Breadcrumb>
          <Breadcrumb.Item>{t('migration')}</Breadcrumb.Item>
          <Breadcrumb.Item>{t('source list')}</Breadcrumb.Item>
        </Breadcrumb>
      </div>

      <div className="mx-4 my-2 p-4 rounded bg-white border-1 border-gray-300 border-dashed whitespace-pre-line">
        {t('source list desc')}
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
                    icon={<PlayCircleOutlined />}
                    key="1"
                    onClick={handleEnableSource}
                  >
                    {t('enable')}
                  </Menu.Item>
                  <Menu.Item
                    icon={<DeleteOutlined />}
                    key="1"
                    onClick={handleRemoveSource}
                  >
                    {t('delete')}
                  </Menu.Item>
                </Menu>
              }
            >
              <Button onClick={handleDisableSource}>
                <PauseCircleOutlined />
                {t('disable')} <DownOutlined />
              </Button>
            </Dropdown>
          </Space>
        </Col>
        <Col span={2}>
          <Button onClick={handleAddNew} icon={<PlusSquareOutlined />}>
            {t('add')}
          </Button>
        </Col>
      </Row>

      <Table
        className="p-4"
        dataSource={result}
        columns={columns}
        loading={isFetching}
        rowKey="source_name"
        rowSelection={rowSelection}
        pagination={{
          total: data?.total,
        }}
      />

      <Modal
        title={currentSource ? t('edit source') : t('add new source')}
        visible={showModal}
        onCancel={handleCancel}
        centered
        footer={null}
        maskClosable={false}
        width={800}
      >
        <CreateOrUpdateSource
          onCancel={handleCancel}
          onSubmit={handleConfirm}
          currentSource={currentSource}
        />
      </Modal>

      <Drawer
        title={t('source detail')}
        placement="right"
        size="large"
        visible={drawerVisible}
        onClose={() => setDrawerVisible(false)}
      >
        {currentSource && !isFetchingTaskListData ? (
          <Tabs defaultActiveKey="1">
            <Tabs.TabPane tab={t('related task')} key="1">
              <Collapse>
                {taskListData?.data.map(item => (
                  <Collapse.Panel
                    showArrow={false}
                    key={item.name}
                    header={<SimpleTaskPanel task={item} preventClick />}
                  />
                ))}
              </Collapse>
            </Tabs.TabPane>
            <Tabs.TabPane tab={t('runtime config')} key="2">
              <SyntaxHighlighter style={ghcolors} language="json">
                {JSON.stringify(currentSource, null, 2)}
              </SyntaxHighlighter>
            </Tabs.TabPane>
          </Tabs>
        ) : (
          <div className="flex items-center justify-center">
            <Spin />
          </div>
        )}
      </Drawer>
    </div>
  )
}

export const meta = {
  title: () => i18n.t('source list'),
  index: 1,
}

export default SourceList

import React, { useCallback, useEffect, useState } from 'react'
import { omit } from 'lodash'
import { useTranslation } from 'react-i18next'

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
} from '~/uikit'
import {
  ImportOutlined,
  ExportOutlined,
  SearchOutlined,
  ExclamationCircleOutlined,
  PlusSquareOutlined,
} from '~/uikit/icons'
import CreateOrUpdateSource from '~/components/CreateOrUpdateSource'
import type { Source } from '~/models/source'
import { unimplemented } from '~/utils/unimplemented'
import {
  useDmapiCreateSourceMutation,
  useDmapiDeleteSourceMutation,
  useDmapiGetSourceListQuery,
} from '~/models/source'

const { confirm } = Modal

const SourceList: React.FC = () => {
  const [t] = useTranslation()
  const [showModal, setShowModal] = useState(false)
  const [currentSource, setCurrentSource] = useState<Source | null>(null)
  const [selectedSources, setSelectedSources] = useState<string[]>([])

  const { data, isFetching } = useDmapiGetSourceListQuery({ withStatus: false })
  const [createSource] = useDmapiCreateSourceMutation()
  const [removeSource] = useDmapiDeleteSourceMutation()

  const handleCancel = useCallback(() => {
    setShowModal(false)
  }, [])

  const handleConfirm = useCallback(
    async (payload: Partial<Source>) => {
      const data = omit(payload, ['relay', 'security'])
      const key = 'createSource-' + Date.now()
      message.loading({ content: t('saving'), key })
      createSource(data)
        .unwrap()
        .then(() => {
          message.success({ content: t('saved'), key, duration: 6 })
          setShowModal(false)
        })
    },
    [currentSource]
  )

  const handleRemoveSource = useCallback(async () => {
    const key = 'removeSource-' + Date.now()

    confirm({
      title: (
        <span>
          {t('confirm to delete')}
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
  }, [selectedSources])

  const dataSource = data?.data
  const columns = [
    {
      title: t('name'),
      dataIndex: 'source_name',
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
            <a
              onClick={() => {
                setCurrentSource(data)
                setShowModal(true)
              }}
            >
              {t('edit')}
            </a>
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
      <Row className="p-4" justify="space-between">
        <Col span={22}>
          <Space>
            <Input
              suffix={<SearchOutlined />}
              placeholder={t('search placeholder')}
            />
            <Button icon={<ExportOutlined />} onClick={unimplemented}>
              {t('export')}
            </Button>
            <Button icon={<ImportOutlined />} onClick={unimplemented}>
              {t('import')}
            </Button>
            {selectedSources.length > 0 && (
              <Button onClick={handleRemoveSource} danger type="primary">
                {t('delete')}
              </Button>
            )}
          </Space>
        </Col>
        <Col span={2}>
          <Button
            onClick={() => setShowModal(true)}
            icon={<PlusSquareOutlined />}
          >
            {t('add')}
          </Button>
        </Col>
      </Row>

      <Table
        className="p-4"
        dataSource={dataSource}
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
    </div>
  )
}

export default SourceList

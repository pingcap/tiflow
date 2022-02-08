import React from 'react'
import { useTranslation } from 'react-i18next'

import {
  Space,
  Button,
  Tabs,
  Card,
  Table,
  Badge,
  message,
  Modal,
  Breadcrumb,
} from '~/uikit'
import {
  RedoOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
} from '~/uikit/icons'
import i18n from '~/i18n'
import {
  useDmapiGetClusterMasterListQuery,
  useDmapiGetClusterWorkerListQuery,
  useDmapiOfflineMasterNodeMutation,
  useDmapiOfflineWorkerNodeMutation,
} from '~/models/cluster'

const { TabPane } = Tabs

const MasterTable: React.FC = () => {
  const [t] = useTranslation()
  const { data, isFetching, refetch } = useDmapiGetClusterMasterListQuery()
  const [offlineMasterNode] = useDmapiOfflineMasterNodeMutation()
  const handleConfirmOffline = (name: string) => {
    Modal.confirm({
      title: t('are you sure to offline this master'),
      icon: <ExclamationCircleOutlined />,
      onOk() {
        const key = 'offlineMasterNode-' + Date.now()
        message.loading({ content: t('requesting'), key })
        offlineMasterNode(name)
          .unwrap()
          .then(() => message.success({ content: t('request success') }))
          .catch(() => message.destroy(key))
      },
    })
  }
  const dataSource = data?.data
  const columns = [
    {
      title: t('name'),
      dataIndex: 'name',
    },
    {
      title: t('address'),
      dataIndex: 'addr',
    },
    {
      title: t('status'),
      dataIndex: 'alive',
      render(alive: boolean) {
        return alive ? <Badge color="green" /> : <Badge color="red" />
      },
    },
    {
      title: t('is leader'),
      dataIndex: 'leader',
      render(leader: boolean) {
        return leader ? (
          <CheckCircleOutlined style={{ color: 'green' }} />
        ) : null
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
              danger
              onClick={() => handleConfirmOffline(name)}
            >
              {t('offline')}
            </Button>
          </Space>
        )
      },
    },
  ]

  return (
    <div>
      <div>
        <Space>
          <Button icon={<RedoOutlined />} onClick={refetch}>
            {t('refresh')}
          </Button>
        </Space>
      </div>

      <Table
        className="py-4"
        dataSource={dataSource}
        columns={columns}
        loading={isFetching}
        rowKey="name"
        pagination={{
          total: data?.total,
        }}
      />
    </div>
  )
}

const WorkerTable: React.FC = () => {
  const [t] = useTranslation()
  const { data, isFetching, refetch } = useDmapiGetClusterWorkerListQuery()
  const [offlineWorkerNode] = useDmapiOfflineWorkerNodeMutation()
  const handleConfirmOffline = (name: string) => {
    Modal.confirm({
      title: t('are you sure to offline this worker'),
      icon: <ExclamationCircleOutlined />,
      onOk() {
        const key = 'offlineWorkerNode-' + Date.now()
        message.loading({ content: t('requesting'), key })
        offlineWorkerNode(name)
          .unwrap()
          .then(() => message.success({ content: t('request success') }))
          .catch(() => message.destroy(key))
      },
    })
  }
  const dataSource = data?.data
  const columns = [
    {
      title: t('name'),
      dataIndex: 'name',
    },
    {
      title: t('address'),
      dataIndex: 'addr',
    },
    {
      title: t('bound stage'),
      dataIndex: 'bound_stage',
      render(bound_stage: string) {
        return (
          <Badge
            status={bound_stage === 'bound' ? 'success' : 'default'}
            text={bound_stage}
          />
        )
      },
    },
    {
      title: t('source'),
      dataIndex: 'bound_source_name',
    },
    {
      title: t('operations'),
      dataIndex: 'name',
      render(name: string) {
        return (
          <Space>
            <Button type="link">{t('edit')}</Button>

            <Button
              type="link"
              danger
              onClick={() => handleConfirmOffline(name)}
            >
              {t('offline')}
            </Button>
          </Space>
        )
      },
    },
  ]

  return (
    <div>
      <div>
        <Space>
          <Button icon={<RedoOutlined />} onClick={refetch}>
            {t('refresh')}
          </Button>
        </Space>
      </div>

      <Table
        className="py-4"
        dataSource={dataSource}
        columns={columns}
        loading={isFetching}
        rowKey="name"
        pagination={{
          total: data?.total,
        }}
      />
    </div>
  )
}

const Member: React.FC = () => {
  const [t] = useTranslation()

  return (
    <div>
      <div className="px-4 pt-4">
        <Breadcrumb>
          <Breadcrumb.Item>{t('cluster management')}</Breadcrumb.Item>
          <Breadcrumb.Item>{t('member list')}</Breadcrumb.Item>
        </Breadcrumb>
      </div>

      <Card className="!m-4">
        <Tabs defaultActiveKey="1">
          <TabPane tab="Master" key="1">
            <MasterTable />
          </TabPane>
          <TabPane tab="Worker" key="2">
            <WorkerTable />
          </TabPane>
        </Tabs>
      </Card>
    </div>
  )
}

export const meta = {
  title: () => i18n.t('member list'),
  index: 0,
}

export default Member

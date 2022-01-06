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
} from '~/uikit'
import {
  RedoOutlined,
  PlusSquareOutlined,
  ExportOutlined,
  ImportOutlined,
  DownOutlined,
  SearchOutlined,
} from '~/uikit/icons'
import i18n from '~/i18n'
import { useDmapiGetTaskConfigListQuery } from '~/models/taskConfig'
import { Task } from '~/models/task'
import { unimplemented } from '~/utils/unimplemented'
import CreateTaskConfig from '~/components/CreateTaskConfig'
import BatchImportTaskConfig from '~/components/BatchImportTaskConfig'
import { useFuseSearch } from '~/utils/search'

const TaskConfig: React.FC = () => {
  const [t] = useTranslation()
  const [isModalVisible, setIsModalVisible] = useState(false)
  const navigate = useNavigate()
  const loc = useLocation()

  const { data, isFetching, refetch } = useDmapiGetTaskConfigListQuery(
    undefined,
    { skip: loc.hash === '#new' }
  )

  const dataSource = data?.data
  const { result, setKeyword } = useFuseSearch(dataSource, {
    keys: ['name'],
  })
  const columns = [
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
      render(data: Task) {
        const { source_config } = data

        if (source_config?.source_conf?.length > 0) {
          return t('{{val}} and other {{count}}', {
            val: source_config.source_conf[0].source_name,
            count: source_config.source_conf.length,
          })
        }

        return '-'
      },
    },
    {
      title: t('target info'),
      render(data: Task) {
        return data.target_config.host
      },
    },
    {
      title: t('migrate rules'),
      render(data: Task) {
        return t('{{count}} rules', {
          count: data.table_migrate_rule.length,
        })
      },
    },
    {
      title: t('event filter'),
      render(data: Task) {
        return t('{{count}} filters', {
          count: Object.keys(data.binlog_filter_rule || {}).length,
        })
      },
    },
    {
      title: t('operations'),
      render() {
        return (
          <Space>
            <Button onClick={() => {}} type="link">
              {t('edit')}
            </Button>
            <Button onClick={() => {}} type="link" danger>
              {t('delete')}
            </Button>
          </Space>
        )
      },
    },
  ]

  if (loc.hash === '#new') {
    return <CreateTaskConfig />
  }

  return (
    <div>
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
              <Button onClick={() => setIsModalVisible(true)}>
                <ImportOutlined />
                {t('import')} <DownOutlined />
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
        pagination={{
          total: data?.total,
        }}
      />

      <Modal
        title={t('import task config')}
        visible={isModalVisible}
        footer={null}
        destroyOnClose
        onCancel={() => setIsModalVisible(false)}
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

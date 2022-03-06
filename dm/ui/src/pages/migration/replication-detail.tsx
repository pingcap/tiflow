import React, { useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'

import i18n from '~/i18n'
import {
  Row,
  Col,
  Button,
  Space,
  Table,
  Breadcrumb,
  TableColumnsType,
} from '~/uikit'
import { RedoOutlined } from '~/uikit/icons'
import { Task, useDmapiGetTaskListQuery } from '~/models/task'

const ReplicationDetail: React.FC = () => {
  const [t] = useTranslation()
  const [currentTask, setCurrentTask] = useState<Task>()
  const { data, isFetching, refetch } = useDmapiGetTaskListQuery({
    withStatus: true,
  })

  const dataSource = data?.data
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
  ]

  useEffect(() => {
    if (!currentTask && data && data.data[0]) {
      setCurrentTask(data.data[0])
    }
  }, [currentTask, data])

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
            <Button icon={<RedoOutlined />} onClick={refetch}>
              {t('refresh')}
            </Button>
          </Space>
        </Col>
      </Row>

      <Table
        className="p-4"
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

export const meta = {
  title: () => i18n.t('replication detail'),
  index: 3,
}

export default ReplicationDetail

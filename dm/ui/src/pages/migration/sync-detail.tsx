import React from 'react'
import { useTranslation } from 'react-i18next'

import i18n from '~/i18n'
import { Row, Col, Button, Space, Table } from '~/uikit'
import { RedoOutlined } from '~/uikit/icons'
import { useDmapiGetTaskListQuery } from '~/models/task'

const SyncDetail: React.FC = () => {
  const [t] = useTranslation()
  const { data, isFetching, refetch } = useDmapiGetTaskListQuery({
    withStatus: true,
  })

  const dataSource = data?.data
  const columns = [
    {
      title: t('task name'),
      dataIndex: 'name',
    },
  ]
  return (
    <div>
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
  title: () => i18n.t('sync detail'),
  index: 3,
}

export default SyncDetail

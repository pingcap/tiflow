import React from 'react'
import { useTranslation } from 'react-i18next'

import i18n from '~/i18n'
import { Input, Row, Col, Button, Space, Table, Breadcrumb } from '~/uikit'
import { RedoOutlined, SearchOutlined } from '~/uikit/icons'
import { useDmapiGetTaskListQuery } from '~/models/task'
import { useFuseSearch } from '~/utils/search'

const ReplicationDetail: React.FC = () => {
  const [t] = useTranslation()
  const { data, isFetching, refetch } = useDmapiGetTaskListQuery({
    withStatus: true,
  })

  const dataSource = data?.data
  const { result, setKeyword } = useFuseSearch(dataSource, {
    keys: ['name'],
  })
  const columns = [
    {
      title: t('task name'),
      dataIndex: 'name',
    },
  ]
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
            <Input
              onChange={e => setKeyword(e.target.value)}
              suffix={<SearchOutlined />}
              placeholder={t('search placeholder')}
            />
            <Button icon={<RedoOutlined />} onClick={refetch}>
              {t('refresh')}
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

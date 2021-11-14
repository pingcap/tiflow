import React, { useState } from 'react'

import { Button, Modal, Space, Input, Col, Row } from '~/uikit'
import {
  IconPlus,
  IconExport,
  IconImport,
  IconTickCircle,
  IconSearch,
} from '~/uikit/icons'
import CreateSource from '~/components/CreateSource'

const UpstreamList: React.FC = () => {
  const [showModal, setShowModal] = useState(false)
  return (
    <div>
      <Row className="p-4" type="flex" justify="space-between">
        <Col span={22}>
          <Space>
            <Input suffix={<IconSearch />} placeholder="搜索" />
            <Button icon={<IconExport />}>导出</Button>
            <Button icon={<IconImport />}>导入</Button>
            <Button icon={<IconTickCircle />}>应用</Button>
            <Button>删除</Button>
          </Space>
        </Col>
        <Col span={2}>
          <Button onClick={() => setShowModal(true)} icon={<IconPlus />}>
            新增
          </Button>
        </Col>
      </Row>

      <Modal
        size="large"
        title="新增上游"
        visible={showModal}
        onOk={() => {}}
        onCancel={() => setShowModal(false)}
        centered
        bodyStyle={{}}
      >
        <CreateSource />
      </Modal>
    </div>
  )
}

export default UpstreamList

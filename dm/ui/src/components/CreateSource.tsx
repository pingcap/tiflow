import React from 'react'

import { Form, Row, Col } from '~/uikit'

const { Section, Input, Switch } = Form

const CreateSource: React.FC = () => {
  return (
    <Form>
      <Row gutter={16}>
        <Col span={12}>
          <Section text="基本信息">
            <Input
              field="source_name"
              label="名称"
              placeholder="mysql-01"
              trigger="blur"
            />

            <Switch field="enable_gtid" label="启用 GTID" />

            <Input field="host" label="地址" placeholder="1.1.1.1" />

            <Input field="port" label="端口" placeholder="3306" />

            <Input field="password" type="password" label="密码" />
          </Section>
        </Col>

        <Col span={12}>
          <Section text="TLS 配置（可选）">
            <Form.Input
              field="ssl_ca_content"
              label="SSL CA"
              placeholder="/path/to/ca.pem"
            />

            <Form.Input
              field="ssl_cert_content"
              label="SSL CERT"
              placeholder="/path/to/cert.pem"
            />

            <Form.Input
              field="ssl_key_content"
              label="SSL KEY"
              placeholder="/path/to/key.pem"
            />
          </Section>

          <Section text="Relay Log 配置（可选）">
            <Switch field="relay_enable" label="启用 Relay Log" />

            <Input
              field="binlog_name"
              label="起始 binlog"
              placeholder="binlog 名称或 GTID"
            />

            <Input
              field="binlog_location"
              label="存放位置"
              placeholder="/location"
            />
          </Section>
        </Col>
      </Row>
    </Form>
  )
}

export default CreateSource

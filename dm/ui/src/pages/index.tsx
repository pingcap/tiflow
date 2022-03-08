import React, { useEffect, useState } from 'react'
import { useLocalStorageState } from 'ahooks'
import { useTranslation } from 'react-i18next'

import { Alert, Modal, Skeleton, Form, Input } from '~/uikit'

const Dashboard = () => {
  const [t] = useTranslation()
  const [config, setDashboardConfig] = useLocalStorageState<{
    port: string
    host: string
  }>('dashboard-config', {
    defaultValue: {
      port: '',
      host: '',
    },
  })
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [form] = Form.useForm()
  const { host, port } = config

  const handleCancel = () => {
    setIsModalOpen(false)
  }
  const handleOk = () => {
    form.validateFields().then(values => {
      setDashboardConfig(values)
      setIsModalOpen(false)
    })
  }

  useEffect(() => {
    if (!config?.host || !config?.port) {
      setIsModalOpen(true)
    }
  }, [config])

  return (
    <div className="h-full w-full">
      <Modal
        title={t('edit config')}
        visible={isModalOpen}
        onOk={handleOk}
        onCancel={handleCancel}
      >
        <Alert
          message={t('note')}
          description={t('note-dashboard-config')}
          type="info"
          showIcon
        />

        <div className="mt-4">
          <Form form={form} initialValues={config}>
            <Form.Item label={t('host')} name="host" required>
              <Input />
            </Form.Item>

            <Form.Item label={t('port')} name="port" required>
              <Input />
            </Form.Item>
          </Form>
        </div>
      </Modal>
      {(!host || !port) && <Skeleton active />}
      {port && host && (
        <iframe
          id="dm-dashboard-grafana-iframe"
          onLoad={() => {
            // always re-open it on load
            // because detecting cross origin iframe content is loaded is infeasible
            setIsModalOpen(true)
          }}
          src={`//${config.host}:${config.port}/d/canCEQgnk/dm-monitor-standard?orgId=1&refresh=1m&theme=light&from=now-12h&to=now&kiosk`}
          className="h-full w-full"
          frameBorder="0"
        />
      )}
    </div>
  )
}

export default Dashboard

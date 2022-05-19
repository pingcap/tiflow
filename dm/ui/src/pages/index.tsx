import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useLocalStorageState } from 'ahooks'
import { useTranslation } from 'react-i18next'
import clsx from 'clsx'

import { ControlOutlined } from '~/uikit/icons'
import {
  Alert,
  Button,
  Modal,
  Skeleton,
  Form,
  Input,
  Popover,
  Select,
} from '~/uikit'

const standard =
  '/d/canCEQgnk/dm-monitor-standard?orgId=1&refresh=1m&theme=light&from=now-12h&to=now&kiosk'
const professional =
  '/d/Ja68YqRnz/dm-monitor-professional?orgId=1&refresh=1m&theme=light&from=now-12h&to=now&kiosk'

const Dashboard = () => {
  const [t] = useTranslation()
  const [config, setDashboardConfig] = useLocalStorageState<{
    port: string
    host: string
    view: string
  }>('dashboard-config', {
    defaultValue: {
      port: '',
      host: '',
      view: standard,
    },
  })
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [loading, setLoading] = useState(true)
  const [form] = Form.useForm()
  const { host, port, view } = config

  const handleCancel = () => {
    setIsModalOpen(false)
  }
  const handleOk = () => {
    form.validateFields().then(values => {
      setDashboardConfig(c => ({ ...c, ...values }))
      setIsModalOpen(false)
    })
  }

  useEffect(() => {
    if (!config?.host || !config?.port) {
      setIsModalOpen(true)
    }
  }, [config])

  return (
    <div className="h-full w-full relative">
      <Popover
        arrowPointAtCenter={false}
        placement="topRight"
        content={
          <div>
            <div className="mt-4">
              <Form
                form={form}
                initialValues={config}
                onFinish={values =>
                  setDashboardConfig(c => ({ ...c, ...values }))
                }
              >
                <Form.Item label={t('host')} name="host">
                  <Input />
                </Form.Item>

                <Form.Item label={t('port')} name="port">
                  <Input />
                </Form.Item>

                <Form.Item label={t('dashboard view')} name="view">
                  <Select>
                    <Select.Option value={standard}>
                      {t('dashboard standard')}
                    </Select.Option>
                    <Select.Option value={professional}>
                      {t('dashboard professional')}
                    </Select.Option>
                  </Select>
                </Form.Item>

                <Button htmlType="submit">{t('save')}</Button>
              </Form>
            </div>
          </div>
        }
      >
        <div
          style={{
            zIndex: 2,
            position: 'fixed',
            bottom: 100,
            right: 40,
            height: 50,
            width: 50,
            lineHeight: '50px',
            textAlign: 'center',
            borderRadius: '50%',
            backgroundColor: '#fff',
            cursor: 'pointer',
            boxShadow:
              '0 3px 6px -4px #0000001f,0 6px 16px #00000014,0 9px 28px 8px #0000000d',
          }}
        >
          <ControlOutlined style={{ fontSize: '18px' }} />
        </div>
      </Popover>
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
      {loading && (
        <div className="m-4 absolute h-full w-full z-1">
          {t('dashboard loading tip')}
          <Skeleton active />
          <Skeleton active />
        </div>
      )}
      {port && host && (
        <iframe
          key={view}
          onLoad={() => setLoading(false)}
          src={`//${host}:${port}${view}`}
          className={clsx('w-full h-full', loading && 'invisible')}
          frameBorder="0"
        />
      )}
    </div>
  )
}

const DashboardV2 = () => {
  const navigate = useNavigate()

  useEffect(() => {
    navigate('/migration/task')
  }, [])

  return null
}

export default DashboardV2

import React from 'react'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'

import { Form, Input, Select, Button } from '~/uikit'
import { OnDuplicateBehavior, TaskMode, TaskShardMode } from '~/models/task'
import { StepCompnent } from '~/components/CreateOrUpdateTask/shared'

const BasicInfo: StepCompnent = ({ initialValues }) => {
  const [t] = useTranslation()
  const navigate = useNavigate()

  return (
    <Form
      wrapperCol={{ span: 16 }}
      labelCol={{ span: 4 }}
      name="basicInfo"
      initialValues={initialValues}
    >
      <Form.Item
        label={t('task name')}
        required
        name="name"
        tooltip={t('create task name tooltip')}
        rules={[
          {
            required: true,
            whitespace: true,
            message: t('task name is required'),
          },
        ]}
      >
        <Input placeholder="task 1" />
      </Form.Item>

      <Form.Item
        label={t('task mode')}
        name="task_mode"
        tooltip={
          <div className="whitespace-pre-line">
            {t('create task mode tooltip')}
          </div>
        }
      >
        <Select placeholder="all">
          {Object.values(TaskMode).map(mode => (
            <Select.Option key={mode} value={mode}>
              {mode}
            </Select.Option>
          ))}
        </Select>
      </Form.Item>

      <Form.Item
        label={t('task shard mode')}
        name="shard_mode"
        tooltip={
          <div className="whitespace-pre-line">
            {t('create task shard mode tooltip')}
          </div>
        }
      >
        <Select placeholder="optimistic">
          {Object.values(TaskShardMode).map(mode => (
            <Select.Option key={mode} value={mode}>
              {mode}
            </Select.Option>
          ))}
        </Select>
      </Form.Item>

      <Form.Item
        label={t('meta db')}
        name="meta_schema"
        tooltip={t('create task meta db tooltip')}
      >
        <Input placeholder="dm_meta" />
      </Form.Item>

      <Form.Item label={t('on duplicate behavior')} name="on_duplicate">
        <Select placeholder="overwrite">
          {Object.values(OnDuplicateBehavior).map(mode => (
            <Select.Option key={mode} value={mode}>
              {mode}
            </Select.Option>
          ))}
        </Select>
      </Form.Item>

      <Form.Item>
        <Button className="mr-4" onClick={() => navigate('/migration/task')}>
          {t('cancel')}
        </Button>

        <Button type="primary" htmlType="submit">
          {t('next')}
        </Button>
      </Form.Item>
    </Form>
  )
}

export default BasicInfo

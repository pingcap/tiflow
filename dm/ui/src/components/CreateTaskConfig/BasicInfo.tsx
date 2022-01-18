import React from 'react'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'

import { Form, Input, Select, Switch, Button } from '~/uikit'
import { OnDuplicateBehavior, TaskMode, TaskShardMode } from '~/models/task'
import { StepCompnent } from '~/components/CreateTaskConfig/shared'

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

      <Form.Item label={t('task mode')} name="task_mode">
        <Select placeholder="all">
          {Object.values(TaskMode).map(mode => (
            <Select.Option key={mode} value={mode}>
              {mode}
            </Select.Option>
          ))}
        </Select>
      </Form.Item>

      <Form.Item label={t('task shard mode')} name="shard_mode">
        <Select placeholder="optimistic">
          {Object.values(TaskShardMode).map(mode => (
            <Select.Option key={mode} value={mode}>
              {mode}
            </Select.Option>
          ))}
        </Select>
      </Form.Item>

      <Form.Item label={t('meta db')} name="meta_schema">
        <Input placeholder="dm_meta" />
      </Form.Item>

      <Form.Item
        label={t('online schema change')}
        name="enhance_online_schema_change"
        valuePropName="checked"
      >
        <Switch defaultChecked={false} />
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
        <Button className="mr-4" onClick={() => navigate('#')}>
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

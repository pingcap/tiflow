import React from 'react'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'

import { Form, Input, Select, Button } from '~/uikit'
import { TaskMode, TaskShardMode } from '~/models/task'
import { StepCompnent } from '~/components/CreateOrUpdateTask/shared'

const BasicInfo: StepCompnent = ({ initialValues, isEditing }) => {
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
        <Input placeholder="task 1" disabled={isEditing} />
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
        <Select placeholder="all" disabled={isEditing}>
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
        <Select placeholder={TaskShardMode.PESSIMISTIC}>
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
        <Input placeholder="dm_meta" disabled={isEditing} />
      </Form.Item>

      <Form.Item
        label={t('data dir')}
        tooltip={t('create task data_dir tooltip')}
        name={['source_config', 'full_migrate_conf', 'data_dir']}
      >
        <Input placeholder="./dump_data " />
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

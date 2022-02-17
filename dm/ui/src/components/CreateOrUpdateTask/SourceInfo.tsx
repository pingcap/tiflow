import React from 'react'
import { useTranslation } from 'react-i18next'

import {
  Button,
  Collapse,
  Form,
  Input,
  InputNumber,
  Select,
  Card,
} from '~/uikit'
import { FileAddOutlined, CloseOutlined } from '~/uikit/icons'
import { useDmapiGetSourceListQuery } from '~/models/source'
import { StepCompnent } from '~/components/CreateOrUpdateTask/shared'

const formLayout = {
  labelCol: { span: 6 },
  wrapperCol: { span: 18 },
}

const itemLayout = {
  labelCol: { span: 4 },
  wrapperCol: { span: 20 },
}

const SourceInfo: StepCompnent = ({ prev, initialValues }) => {
  const [t] = useTranslation()
  const { data, isFetching } = useDmapiGetSourceListQuery({ withStatus: false })

  return (
    <Form {...formLayout} name="sourceInfo" initialValues={initialValues}>
      <Collapse>
        <Collapse.Panel header={t('sync config')} key="1">
          <div className="flex">
            <div className="flex-1">
              <h3 className="max-w-[33%] text-right font-bold mb-8">
                {t('full migrate config')}
              </h3>
              <Form.Item
                label={t('export concurrency')}
                tooltip={t('create task export_threads tooltip')}
                name={['source_config', 'full_migrate_conf', 'export_threads']}
              >
                <InputNumber className="!w-[100%]" placeholder="4" />
              </Form.Item>
              <Form.Item
                label={t('import concurrency')}
                tooltip={t('create task import_threads tooltip')}
                name={['source_config', 'full_migrate_conf', 'import_threads']}
              >
                <InputNumber className="!w-[100%]" placeholder="4" />
              </Form.Item>
              <Form.Item
                label={t('data dir')}
                tooltip={t('create task data_dir tooltip')}
                name={['source_config', 'full_migrate_conf', 'data_dir']}
              >
                <Input placeholder="/data" />
              </Form.Item>
              <Form.Item
                label={t('consistency requirement')}
                name={['source_config', 'full_migrate_conf', 'consistency']}
              >
                <Input />
              </Form.Item>
            </div>
            <div className="flex-1">
              <h3 className="max-w-[33%] text-right font-bold mb-8">
                {t('incremental migrate config')}
              </h3>
              <Form.Item
                label={t('synchronous concurrency')}
                tooltip={t('create task repl_threads tooltip')}
                name={['source_config', 'incr_migrate_conf', 'repl_threads']}
              >
                <InputNumber className="!w-[100%]" placeholder="32" />
              </Form.Item>
              <Form.Item
                label={t('transaction batch')}
                tooltip={t('create task repl_batch tooltip')}
                name={['source_config', 'incr_migrate_conf', 'repl_batch']}
              >
                <InputNumber className="!w-[100%]" placeholder="100" />
              </Form.Item>
            </div>
          </div>
        </Collapse.Panel>
      </Collapse>

      <div className="grid grid-cols-2 gap-4 auto-rows-fr my-4">
        <Form.List name={['source_config', 'source_conf']}>
          {(fields, { add, remove }) => (
            <>
              {fields.map(field => (
                <Card
                  hoverable
                  key={field.key}
                  className="!h-200px relative !border-[#d9d9d9] group"
                >
                  <CloseOutlined
                    onClick={() => remove(field.name)}
                    className="!text-gray-500 absolute top-2 right-2 group-hover:opacity-100 opacity-0 transition"
                  />
                  <Form.Item
                    {...itemLayout}
                    label={t('source name')}
                    name={[field.name, 'source_name']}
                    rules={[
                      { required: true, message: t('source name is required') },
                    ]}
                    fieldKey={[field.fieldKey, 'source_name']}
                  >
                    <Select placeholder="mysql-01" loading={isFetching}>
                      {data?.data.map(source => (
                        <Select.Option
                          key={source.source_name}
                          value={source.source_name}
                        >
                          {source.source_name}
                        </Select.Option>
                      ))}
                    </Select>
                  </Form.Item>

                  <Form.Item
                    {...itemLayout}
                    label={t('binlog')}
                    tooltip={t('create task binlog_name tooltip')}
                  >
                    <Input.Group compact>
                      <Form.Item noStyle name={[field.name, 'binlog_name']}>
                        <Input
                          className="!mr-[5%]"
                          style={{ width: '50%' }}
                          placeholder="name"
                        />
                      </Form.Item>

                      <Form.Item noStyle name={[field.name, 'binlog_pos']}>
                        <InputNumber
                          style={{ width: '45%' }}
                          placeholder="position"
                        />
                      </Form.Item>
                    </Input.Group>
                  </Form.Item>

                  <Form.Item
                    {...itemLayout}
                    label={t('gtid')}
                    name={[field.name, 'binlog_gtid']}
                    fieldKey={[field.fieldKey, 'binlog_gtid']}
                  >
                    <Input />
                  </Form.Item>
                </Card>
              ))}

              <Button
                className="!h-200px"
                type="dashed"
                icon={<FileAddOutlined />}
                onClick={() => add()}
              >
                {t('add source config')}
              </Button>
            </>
          )}
        </Form.List>
      </div>

      <Form.Item>
        <Button className="mr-4" onClick={prev}>
          {t('previous')}
        </Button>
        <Button type="primary" htmlType="submit">
          {t('next')}
        </Button>
      </Form.Item>
    </Form>
  )
}

export default SourceInfo

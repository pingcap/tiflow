import React, { useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { pick } from 'lodash-es'

import { Form, Button, Card, Input, Select } from '~/uikit'
import { FileAddOutlined, CloseOutlined } from '~/uikit/icons'
import { StepCompnent } from '~/components/CreateOrUpdateTask/shared'
import { supportedIgnorableEvents, TaskFormData } from '~/models/task'

const itemLayout = {
  labelCol: { span: 5 },
  wrapperCol: { span: 19 },
}

const EventFilters: StepCompnent = ({ prev, initialValues }) => {
  const [t] = useTranslation()
  const [form] = Form.useForm()

  const handleSubmit = useCallback(() => {
    const rules = form.getFieldValue(
      'binlog_filter_rule_array'
    ) as TaskFormData['binlog_filter_rule_array']

    if (rules && rules.length > 0) {
      form.setFieldsValue({
        binlog_filter_rule_array: rules,
        binlog_filter_rule: rules.reduce(
          (acc, next) => ({
            ...acc,
            [next.name]: pick(next, ['ignore_event', 'ignore_sql']),
          }),
          // eslint-disable-next-line @typescript-eslint/consistent-type-assertions
          {} as TaskFormData['binlog_filter_rule']
        ),
      })
    }

    form.submit()
  }, [form])

  return (
    <Form
      initialValues={initialValues}
      name="eventFilters"
      form={form}
      validateTrigger="onBlur"
    >
      <div className="grid grid-cols-2 gap-4 auto-rows-fr my-4">
        <Form.Item name={['binlog_filter_rule']} hidden>
          <Input />
        </Form.Item>
        <Form.List name={['binlog_filter_rule_array']}>
          {(fields, { add, remove }) => (
            <>
              {fields.map(field => (
                <Card
                  hoverable
                  key={field.key}
                  className="h-200px relative !border-[#d9d9d9] group"
                >
                  <CloseOutlined
                    onClick={() => remove(field.name)}
                    className="!text-gray-500 absolute top-2 right-2 group-hover:opacity-100 opacity-0 transition"
                  />

                  <Form.Item
                    {...itemLayout}
                    label={t('event filter name')}
                    name={[field.name, 'name']}
                    tooltip={t('create task event filter name tooltip')}
                    rules={[
                      {
                        required: true,
                        message: t('event filter name is required'),
                      },
                      {
                        validator: (_, value) => {
                          const existingRules = (form.getFieldValue(
                            'binlog_filter_rule_array'
                          ) as TaskFormData['binlog_filter_rule_array'])!.map(
                            i => i.name
                          )

                          return existingRules.filter(name => name === value)
                            .length >= 2
                            ? Promise.reject(
                                new Error(
                                  t('event filter name should be unique')
                                )
                              )
                            : Promise.resolve()
                        },
                      },
                    ]}
                  >
                    <Input />
                  </Form.Item>

                  <Form.Item
                    {...itemLayout}
                    label={t('event filter ignore event')}
                    name={[field.name, 'ignore_event']}
                    tooltip={t('create task event filter ignore_event tooltip')}
                  >
                    <Select
                      mode="multiple"
                      allowClear
                      placeholder="Please select"
                    >
                      {supportedIgnorableEvents.map(event => (
                        <Select.Option key={event} value={event}>
                          {event}
                        </Select.Option>
                      ))}
                    </Select>
                  </Form.Item>

                  <Form.Item
                    {...itemLayout}
                    label={t('event filter ignore sql')}
                    tooltip={t('create task event filter ignore_sql tooltip')}
                    name={[field.name, 'ignore_sql']}
                  >
                    <Select mode="tags" />
                  </Form.Item>
                </Card>
              ))}

              <Button
                className="!h-200px"
                type="dashed"
                icon={<FileAddOutlined />}
                onClick={() => add()}
              >
                {t('add event filter')}
              </Button>
            </>
          )}
        </Form.List>
      </div>

      <Form.Item>
        <Button className="mr-4" onClick={prev}>
          {t('previous')}
        </Button>
        <Button type="primary" onClick={handleSubmit}>
          {t('next')}
        </Button>
      </Form.Item>
    </Form>
  )
}

export default EventFilters

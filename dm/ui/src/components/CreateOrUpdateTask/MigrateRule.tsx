import React, { useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { Form, Button, Card, Select, Input, Cascader } from '~/uikit'
import {
  FileAddOutlined,
  DoubleRightOutlined,
  CloseOutlined,
  DatabaseOutlined,
  LoadingOutlined,
} from '~/uikit/icons'
import { StepCompnent } from '~/components/CreateOrUpdateTask/shared'
import {
  useDmapiGetSourceSchemaListQuery,
  useDmapiGetSourceListQuery,
  useDmapiGetSourceTableListMutation,
} from '~/models/source'

const itemLayout = {
  labelCol: { span: 6 },
  wrapperCol: { span: 18 },
}

const createPattern = (name: string[]) =>
  name.length === 0 ? '*' : name.join('|')

const MigrateRule: StepCompnent = ({ prev, initialValues }) => {
  const [t] = useTranslation()
  const [currentSource, setCurrentSource] = useState('')
  const [cascaderValue, setCascaderValue] = useState<string[][]>([])
  const [form] = Form.useForm()
  const { data } = useDmapiGetSourceListQuery({ with_status: false })
  const [getSourceTable] = useDmapiGetSourceTableListMutation()
  const { data: schemas, isFetching } = useDmapiGetSourceSchemaListQuery(
    { sourceName: currentSource },
    { skip: !currentSource }
  )

  const [cascaderOptions, setCascaderOptions] = useState(
    schemas?.map(item => ({
      value: item,
      label: item,
      isLeaf: false,
    })) ?? []
  )

  const loadCascaderData = async (selectedOptions: any) => {
    const targetOption = selectedOptions[selectedOptions.length - 1]
    targetOption.loading = true
    const res = await getSourceTable({
      sourceName: currentSource,
      schemaName: targetOption.value,
    }).unwrap()
    targetOption.loading = false
    targetOption.children = res.map(item => ({
      value: item,
      label: item,
      isLeaf: true,
    }))
    setCascaderOptions([...cascaderOptions])
  }

  const handelSchemaSelectInCascader = (value: string[][], field: any) => {
    setCascaderValue(value)
    const dbPattern = createPattern([
      ...new Set(value.map((item: string[]) => item[0])),
    ] as string[])
    const tablePattern = createPattern([
      ...new Set(value.map((item: string[]) => item[1]).filter(Boolean)),
    ] as string[])
    const newData = [...form.getFieldValue('table_migrate_rule')]
    newData[field.key] = {
      ...newData[field.key],
      source: {
        ...newData[field.key].source,
        schema: dbPattern,
        table: tablePattern,
      },
    }
    form.setFieldsValue({
      table_migrate_rule: newData,
    })
  }

  useEffect(() => {
    if (schemas) {
      setCascaderOptions(
        schemas?.map(item => ({
          value: item,
          label: item,
          isLeaf: false,
        }))
      )

      setCascaderValue([])
    }
  }, [schemas])

  return (
    <Form initialValues={initialValues} name="migrateRule" form={form}>
      <div className="grid grid-cols-1 gap-4 auto-rows-fr my-4">
        <Form.List name={['table_migrate_rule']}>
          {(fields, { add, remove }) => (
            <>
              {fields.map(field => (
                <Card
                  hoverable
                  key={field.key}
                  className="min-h-200px relative !border-[#d9d9d9] group"
                >
                  <CloseOutlined
                    onClick={() => remove(field.name)}
                    className="!text-gray-500 absolute top-2 right-2 group-hover:opacity-100 opacity-0 transition"
                  />
                  <div className="grid grid-cols-26">
                    <div className="col-span-8">
                      <h1 className="font-bold">{t('source')}</h1>
                      <Form.Item
                        label={t('source')}
                        name={[field.key, 'source', 'source_name']}
                        {...itemLayout}
                      >
                        <Select
                          onChange={val => setCurrentSource(val as string)}
                        >
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
                        label={t('database')}
                        tooltip={t(
                          'create task migrate rule source schema tooltip'
                        )}
                        name={[field.key, 'source', 'schema']}
                        {...itemLayout}
                      >
                        <Input
                          placeholder="*"
                          addonAfter={
                            <Cascader
                              options={cascaderOptions}
                              loadData={loadCascaderData}
                              multiple
                              maxTagCount="responsive"
                              value={cascaderValue}
                              onChange={(val: any) =>
                                handelSchemaSelectInCascader(val, field)
                              }
                            >
                              {isFetching ? (
                                <LoadingOutlined />
                              ) : (
                                <DatabaseOutlined />
                              )}
                            </Cascader>
                          }
                        />
                      </Form.Item>
                      <Form.Item
                        label={t('table')}
                        tooltip={t(
                          'create task migrate rule source table tooltip'
                        )}
                        name={[field.key, 'source', 'table']}
                        {...itemLayout}
                      >
                        <Input placeholder="tbl_*" />
                      </Form.Item>
                    </div>

                    <div className="flex items-center justify-center col-span-1">
                      <DoubleRightOutlined />
                    </div>

                    <div className="col-span-8">
                      <h1 className="font-bold">{t('event filter')}</h1>
                      <Form.Item
                        label={t('event filter')}
                        {...itemLayout}
                        name={[field.key, 'binlog_filter_rule']}
                        tooltip={t(
                          'create task migrate rule binlog_filter_rule tooltip'
                        )}
                      >
                        <Select
                          mode="multiple"
                          allowClear
                          placeholder="Please select filter"
                        >
                          {initialValues?.binlog_filter_rule_array?.map(
                            rule => (
                              <Select.Option key={rule.name} value={rule.name}>
                                {rule.name}
                              </Select.Option>
                            )
                          )}
                        </Select>
                      </Form.Item>
                    </div>

                    <div className="flex items-center justify-center col-span-1">
                      <DoubleRightOutlined />
                    </div>

                    <div className="col-span-8">
                      <h1 className="font-bold">{t('target')}</h1>
                      <Form.Item label={t('target')} {...itemLayout}>
                        <Input
                          value={initialValues?.target_config.host}
                          disabled
                        />
                      </Form.Item>
                      <Form.Item
                        label={t('database')}
                        tooltip={t(
                          'create task migrate rule target schema tooltip'
                        )}
                        name={[field.key, 'target', 'schema']}
                        {...itemLayout}
                      >
                        <Input />
                      </Form.Item>
                      <Form.Item
                        label={t('table')}
                        tooltip={t(
                          'create task migrate rule target table tooltip'
                        )}
                        name={[field.key, 'target', 'table']}
                        {...itemLayout}
                      >
                        <Input />
                      </Form.Item>
                    </div>
                  </div>
                </Card>
              ))}

              <Button
                className="!h-200px"
                type="dashed"
                icon={<FileAddOutlined />}
                onClick={() => add()}
              >
                {t('add migrate rule')}
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
          {t('save')}
        </Button>
      </Form.Item>
    </Form>
  )
}

export default MigrateRule

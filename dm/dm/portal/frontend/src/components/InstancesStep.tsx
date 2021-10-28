import React, { useState, useContext } from 'react'
import styled from 'styled-components'
import { Form, Input, Icon, Button, Divider, message } from 'antd'
import { FormattedMessage, useIntl } from 'react-intl'
import {
  IPageAction,
  ITargetInstance,
  ISourceInstance,
  IDatabase,
  IInstances,
  IBinlogMeta,
  IOriginSourceInstance,
  ISourceConfig
} from '../types'
import { checkTargetInstance, checkSourceInstance } from '../services/api'
import {
  convertSourceInstances,
  genDefSourceInstance
} from '../utils/config-util'
import { EditContext } from '../App'

const Container = styled.div`
  max-width: 800px;
  margin: 0 auto;

  .config-instances-form .ant-form-item {
    display: flex;
    margin: 0;
    margin-left: 10px;
  }
  .instance-item {
    border: 1px solid #ccc;
    border-radius: 4px;
    padding: 10px;
    margin-bottom: 20px;
  }
  .instance-item-row {
    display: flex;
    align-items: center;
  }
  .add-button {
    text-align: center;

    button {
      width: 40%;
    }
  }
  .action-buttons {
    display: flex;
    justify-content: center;

    button {
      margin: 20px;
    }
  }
  .delete-icon {
    cursor: pointer;
    font-size: 24px;
    margin-left: 8px;
    color: #999;
    transition: all 0.3s;
  }
  .delete-icon:hover {
    color: #777;
  }
`

type DatabaseConfigProps = {
  dbConfig: IDatabase
  onData: (config: IDatabase) => void
}

function DatabaseConfig({ dbConfig, onData }: DatabaseConfigProps) {
  const intl = useIntl()
  const edit = useContext(EditContext)

  const [config, setConfig] = useState<IDatabase>(dbConfig)
  const { host, port, user, password } = config

  function changeHandler(e: any) {
    const name = e.target.name
    let value = e.target.value
    if (name === 'port') {
      value = parseInt(value) || 0
    }
    const newConfig: IDatabase = {
      ...config,
      [name]: value
    }
    setConfig(newConfig)
    onData(newConfig)
  }

  return (
    <div className="instance-item-row">
      <Form.Item label="IP:">
        <Input
          disabled={edit}
          placeholder="192.168.0.1"
          value={host}
          name="host"
          onChange={changeHandler}
        />
      </Form.Item>
      <Form.Item label={intl.formatMessage({ id: 'port' })}>
        <Input
          disabled={edit}
          placeholder="3306"
          value={port}
          name="port"
          onChange={changeHandler}
        />
      </Form.Item>
      <Form.Item label={intl.formatMessage({ id: 'user' })}>
        <Input
          disabled={edit}
          placeholder="root"
          value={user}
          name="user"
          onChange={changeHandler}
        />
      </Form.Item>
      <Form.Item label={intl.formatMessage({ id: 'pwd' })}>
        <Input
          disabled={edit}
          placeholder="pwd"
          value={password}
          name="password"
          type="password"
          onChange={changeHandler}
        />
      </Form.Item>
    </div>
  )
}

type BinlogConfigProps = {
  binlogMeta: IBinlogMeta
  onData: (binlog: IBinlogMeta) => void
}

function BinlogConfig({ binlogMeta, onData }: BinlogConfigProps) {
  const [binlog, setBinlog] = useState<IBinlogMeta>(binlogMeta)
  const { binlogName, binlogPos } = binlog

  function changeHandler(e: any) {
    const name = e.target.name
    let value = e.target.value
    if (name === 'binlogPos') {
      value = parseInt(value) || 0
    }
    const newBinlog = {
      ...binlog,
      [name]: value
    }
    setBinlog(newBinlog)
    onData(newBinlog)
  }

  return (
    <>
      <Form.Item label="binlog-file:">
        <Input
          placeholder="binlog-0001"
          value={binlogName}
          name="binlogName"
          onChange={changeHandler}
        />
      </Form.Item>
      <Form.Item label="binlog-pos:">
        <Input
          placeholder="4"
          value={binlogPos}
          name="binlogPos"
          onChange={changeHandler}
        />
      </Form.Item>
    </>
  )
}

type SourceInstanceItemProps = {
  instance: ISourceInstance
  showBinlog: boolean
  showDelIcon: boolean
  onDelItem: (uuid: number) => void
  onUpdateItem: (instance: ISourceInstance) => void
}

function SourceInstanceItem({
  instance,
  showBinlog,
  showDelIcon,
  onDelItem,
  onUpdateItem
}: SourceInstanceItemProps) {
  const edit = useContext(EditContext)

  const [sourceInstance, setSourceInstance] = useState<ISourceInstance>(
    instance
  )
  const { sourceId, binlogMeta, dbConfig, uuid } = sourceInstance

  function updateDbConfig(dbConfig: IDatabase) {
    const newSourceInstance = {
      ...sourceInstance,
      dbConfig
    }
    setSourceInstance(newSourceInstance)
    onUpdateItem(newSourceInstance)
  }

  function updateBinlogMeta(binlogMeta: IBinlogMeta) {
    const newSourceInstance = {
      ...sourceInstance,
      binlogMeta
    }
    setSourceInstance(newSourceInstance)
    onUpdateItem(newSourceInstance)
  }

  function changeHandler(e: any) {
    const newSourceInstance = {
      ...sourceInstance,
      [e.target.name]: e.target.value
    }
    setSourceInstance(newSourceInstance)
    onUpdateItem(newSourceInstance)
  }

  return (
    <div className="instance-item">
      <div className="instance-item-row">
        <Form.Item label="source-id">
          <Input
            disabled={edit}
            placeholder="replica-01"
            value={sourceId}
            name="sourceId"
            onChange={changeHandler}
          />
        </Form.Item>
        {showBinlog && (
          <BinlogConfig binlogMeta={binlogMeta} onData={updateBinlogMeta} />
        )}
        {showDelIcon && (
          <Icon
            type="minus-circle-o"
            className="delete-icon"
            onClick={() => onDelItem(uuid)}
          />
        )}
      </div>
      <DatabaseConfig dbConfig={dbConfig} onData={updateDbConfig} />
    </div>
  )
}

type InstancesStepProps = IPageAction<IInstances> & {
  instances: IInstances
  showBinlog: boolean
  onFetchSourceConfig: (sourceConfig: ISourceConfig) => void
}

function InstancesStep({
  onNext,
  onPrev,
  onData,
  instances,
  showBinlog,
  onFetchSourceConfig
}: InstancesStepProps) {
  const intl = useIntl()
  const edit = useContext(EditContext)

  const [targetInstance, setTargetInstance] = useState<ITargetInstance>(
    instances.targetInstance
  )
  const [sourceInstances, setSourceInstances] = useState<ISourceInstance[]>(
    instances.sourceInstances
  )
  const [loading, setLoading] = useState(false)

  function addSourceInstance() {
    const newSourceInstances = [...sourceInstances, genDefSourceInstance()]
    setSourceInstances(newSourceInstances)
    onData &&
      onData({
        targetInstance,
        sourceInstances: newSourceInstances
      })
  }

  function delSourceInstance(uuid: number) {
    const newSourceInstances = sourceInstances.filter(
      item => item.uuid !== uuid
    )
    setSourceInstances(newSourceInstances)
    onData &&
      onData({
        targetInstance,
        sourceInstances: newSourceInstances
      })
  }

  function updateSourceInstance(instance: ISourceInstance) {
    const newSourceInstances = sourceInstances.map(item =>
      item.uuid === instance.uuid ? instance : item
    )
    setSourceInstances(newSourceInstances)
    onData &&
      onData({
        targetInstance,
        sourceInstances: newSourceInstances
      })
  }

  function updateTargetInstance(instance: ITargetInstance) {
    setTargetInstance(instance)
    onData &&
      onData({
        targetInstance: instance,
        sourceInstances
      })
  }

  async function handleSubmit() {
    // 如果处于编辑规则状态，直接下一步，不再从服务器读取库表结构
    if (edit) {
      onNext()
      return
    }

    // 否则，从服务器读取库表结构
    // 第一步，validate inputs
    // TODO

    // 第二步，确定下游参数正确
    setLoading(true)
    const res = await checkTargetInstance(targetInstance)
    if (res.err) {
      message.error(
        intl.formatMessage(
          { id: 'downstream_fail' },
          { errMsg: res.err.message }
        )
      )
      setLoading(false)
      return
    }

    const originSourceInstances: IOriginSourceInstance[] = []
    // 第三步，分别请求上游实例，得到其库表结构
    for (let i = 0; i < sourceInstances.length; i++) {
      const instance = sourceInstances[i]
      const res = await checkSourceInstance(instance)
      if (res.err) {
        message.error(
          intl.formatMessage(
            { id: 'upstream_fail' },
            { sourceId: instance.sourceId, errMsg: res.err.message }
          )
        )
        setLoading(false)
        return
      }
      originSourceInstances.push({
        sourceId: instance.sourceId,
        schemas: res.data!.tables
      })
    }
    setLoading(false)

    const sourceConfig = convertSourceInstances(originSourceInstances)
    onFetchSourceConfig(sourceConfig)
    onNext()
  }

  return (
    <Container>
      <Form className="config-instances-form">
        <h2>
          <FormattedMessage id="upstream" />
        </h2>
        {sourceInstances.map(item => (
          <SourceInstanceItem
            key={item.uuid}
            instance={item}
            showBinlog={showBinlog}
            showDelIcon={sourceInstances.length > 1 && !edit}
            onDelItem={delSourceInstance}
            onUpdateItem={updateSourceInstance}
          />
        ))}
        {!edit && (
          <div className="add-button">
            <Button type="dashed" onClick={addSourceInstance}>
              <Icon type="plus" />
              <FormattedMessage id="add" />
            </Button>
          </div>
        )}

        <Divider />

        <h2>
          <FormattedMessage id="downstream" />
        </h2>
        <div className="instance-item">
          <DatabaseConfig
            dbConfig={targetInstance}
            onData={updateTargetInstance}
          />
        </div>

        <div className="action-buttons">
          <Button onClick={() => onPrev()}>
            <FormattedMessage id="pre" />
          </Button>
          <Button
            type="primary"
            htmlType="submit"
            onClick={handleSubmit}
            loading={loading}
          >
            <FormattedMessage id="next" />
          </Button>
        </div>
      </Form>
    </Container>
  )
}

export default InstancesStep

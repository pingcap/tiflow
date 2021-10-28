import React, { useState, useContext } from 'react'
import styled from 'styled-components'
import { Form, Input, Radio, Button } from 'antd'
import { FormattedMessage, useIntl } from 'react-intl'
import { IPageAction, ITaskInfo } from '../types'
import { EditContext } from '../App'

const formItemLayout = {
  labelCol: {
    xs: { span: 24 },
    sm: { span: 8 }
  },
  wrapperCol: {
    xs: { span: 24 },
    sm: { span: 16 }
  }
}

const tailItemLayout = {
  wrapperCol: {
    xs: { span: 24, offset: 0 },
    sm: { span: 16, offset: 8 }
  }
}

const Container = styled.div`
  max-width: 400px;
  margin: 0 auto;

  button {
    margin-top: 10px;
    margin-right: 40px;
  }
`

type Props = IPageAction<ITaskInfo> & {
  taskInfo: ITaskInfo
}

type TaskName = {
  status: 'success' | 'error'
  errMsg: string
  value: string
}

function NamingStep({ onNext, onPrev, onData, taskInfo }: Props) {
  const intl = useIntl()
  const edit = useContext(EditContext)
  const [taskMode, setTaskMode] = useState(taskInfo.taskMode)
  const [taskName, setTaskName] = useState<TaskName>(() =>
    handleTaskNameChange(taskInfo.taskName)
  )

  function handleTaskNameChange(name: string): TaskName {
    if (name.length === 0 || /^[a-zA-Z0-9$_]+$/.test(name)) {
      return {
        status: 'success',
        errMsg: '',
        value: name
      }
    } else {
      return {
        status: 'error',
        errMsg: intl.formatMessage({ id: 'invalid_task_name' }),
        value: name
      }
    }
  }

  return (
    <Container>
      <Form {...formItemLayout}>
        <Form.Item
          label={intl.formatMessage({ id: 'task_name' })}
          validateStatus={taskName.status}
          help={taskName.errMsg}
        >
          <Input
            placeholder="test"
            value={taskName.value}
            onChange={(e: any) =>
              setTaskName(handleTaskNameChange(e.target.value))
            }
          />
        </Form.Item>
        <Form.Item label={intl.formatMessage({ id: 'sync_mode' })}>
          <Radio.Group
            disabled={edit}
            onChange={(e: any) => setTaskMode(e.target.value)}
            value={taskMode}
          >
            <Radio value="full">
              <FormattedMessage id="full_mode" />
            </Radio>
            <Radio value="incremental">
              <FormattedMessage id="inc_mode" />
            </Radio>
            <Radio value="all">All</Radio>
          </Radio.Group>
        </Form.Item>
        <Form.Item {...tailItemLayout}>
          <Button onClick={() => onPrev()}>
            <FormattedMessage id="cancel" />
          </Button>
          <Button
            type="primary"
            htmlType="submit"
            onClick={() => {
              onNext()
              onData && onData({ taskName: taskName.value, taskMode })
            }}
            disabled={
              taskName.value.length === 0 || taskName.status === 'error'
            }
          >
            <FormattedMessage id="next" />
          </Button>
        </Form.Item>
      </Form>
    </Container>
  )
}

export default NamingStep

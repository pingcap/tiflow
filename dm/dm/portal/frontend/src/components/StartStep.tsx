import React from 'react'
import { Button } from 'antd'
import styled from 'styled-components'
import { FormattedMessage } from 'react-intl'

const Container = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;

  h1 {
    line-height: 48px;
    margin-bottom: 48px;
  }

  button {
    margin-bottom: 32px;
  }
`

type Props = {
  onNewRule: () => void
  onEditRule: () => void
}

function StartStep(props: Props) {
  return (
    <Container>
      <h1>
        <FormattedMessage id="dm_task_config_generator" />
      </h1>
      <Button onClick={() => props.onNewRule()}>
        <FormattedMessage id="new_task_config" />
      </Button>
      {/* 编辑功能目前看没有使用场景，先隐藏，后续再移除 */}
      {/* <Button onClick={() => props.onEditRule()}>编辑任务配置</Button> */}
    </Container>
  )
}

export default StartStep

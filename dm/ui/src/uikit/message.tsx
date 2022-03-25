import React from 'react'
import { message as antdMessage, MessageArgsProps } from 'antd'
import { CloseOutlined } from '@ant-design/icons'

const error = (props: Pick<MessageArgsProps, 'content' | 'duration'>) => {
  let hide: () => void

  const onClose = () => {
    hide()
  }

  hide = antdMessage.error({
    ...props,
    content: (
      <span>
        {props.content}
        <CloseOutlined
          onClick={onClose}
          style={{ color: '#999', cursor: 'pointer', marginLeft: 6 }}
        />
      </span>
    ),
  })
}

export const message = {
  ...antdMessage,
  error,
}

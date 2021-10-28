import React from 'react'
import { Upload, Button, Icon, message } from 'antd'
import { UploadProps, UploadChangeParam } from 'antd/lib/upload'
import styled from 'styled-components'
import { IPageAction, IFinalConfig } from '../types'
import { parseFinalConfig } from '../utils/config-util'

const { Dragger } = Upload

const Container = styled.div`
  max-width: 800px;
  margin: 0 auto;

  .action-buttons {
    display: flex;
    justify-content: center;

    button {
      margin: 24px;
      margin-top: 48px;
    }
  }
`

type Props = IPageAction<any>

function UploadStep({ onPrev, onNext, onData }: Props) {
  const uploadProps: UploadProps = {
    name: 'taskfile',
    accept: '.yaml',
    action: '/analyze_config_file',
    showUploadList: false,
    onChange(info: UploadChangeParam) {
      const { status } = info.file
      if (status === 'done') {
        const finalConfig = info.file.response.config as IFinalConfig
        const parsedData = parseFinalConfig(finalConfig)
        console.log(parsedData)

        message.success(
          `${info.file.name} file uploaded successfully.`,
          1,
          () => {
            onData && onData(parsedData)
            onNext()
          }
        )
      } else if (status === 'error') {
        message.error(
          `${info.file.name} file upload failed. error: ${info.file.response.error}`
        )
      }
    }
  }

  return (
    <Container>
      <Dragger {...uploadProps}>
        <p className="ant-upload-drag-icon">
          <Icon type="inbox" />
        </p>
        <p className="ant-upload-text">点击或将文件拖拽到这里上传</p>
        <p className="ant-upload-hint">仅支持 YAML 格式</p>
      </Dragger>
      <div className="action-buttons">
        <Button onClick={() => onPrev()}>取消</Button>
      </div>
    </Container>
  )
}

export default UploadStep

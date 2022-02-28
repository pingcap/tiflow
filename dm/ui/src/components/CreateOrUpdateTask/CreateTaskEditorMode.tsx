import React, { useEffect, useState } from 'react'
import AceEditor from 'react-ace'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'

import { Space, Button } from '~/uikit'
import { TaskFormData } from '~/models/task'

import 'ace-builds/src-noconflict/mode-json'
import 'ace-builds/src-noconflict/theme-tomorrow'

const CreateTaskEditorMode: React.FC<{
  initialValues?: TaskFormData
  onSubmit: (data: TaskFormData) => void
}> = ({ onSubmit, initialValues }) => {
  const [t] = useTranslation()
  const navigate = useNavigate()
  const [val, setVal] = useState(JSON.stringify(initialValues, null, 2))
  const handleEditorChange = (v: string) => setVal(v)
  const handleCancel = () => navigate('/migration/task')
  const handleFormat = () => {
    setVal(JSON.stringify(JSON.parse(val), null, 2))
  }
  const handleSubmit = () => {
    let data: TaskFormData
    try {
      data = JSON.parse(val) as TaskFormData
      onSubmit(data)
    } catch (e) {
      if (e instanceof SyntaxError) {
        // TODO add json validator for editor
      }
    }
  }

  useEffect(() => {
    setVal(JSON.stringify(initialValues, null, 2))
  }, [initialValues])

  return (
    <div className="h-full flex flex-col p-4">
      <div className="mb-4 flex flex-row-reverse">
        <Space>
          <Button onClick={handleCancel}>{t('cancel')}</Button>
          <Button onClick={handleFormat}>{t('format')}</Button>
          <Button type="primary" onClick={handleSubmit}>
            {t('submit')}
          </Button>
        </Space>
      </div>
      <AceEditor
        className="flex-1"
        mode="json"
        theme="tomorrow"
        width="100%"
        fontSize={14}
        name="task-ace-editor"
        value={val}
        onChange={handleEditorChange}
        editorProps={{ $blockScrolling: true }}
        showPrintMargin={false}
        setOptions={{
          enableBasicAutocompletion: false,
          enableLiveAutocompletion: false,
          enableSnippets: false,
          showLineNumbers: true,
          tabSize: 2,
        }}
      />
    </div>
  )
}

export default CreateTaskEditorMode

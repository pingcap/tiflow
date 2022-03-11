import React, { useEffect, useState } from 'react'
import AceEditor from 'react-ace'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'

import { Space, Button } from '~/uikit'
import { TaskFormData, useDmapiConverterTaskMutation } from '~/models/task'

import 'ace-builds/src-noconflict/mode-yaml'
import 'ace-builds/src-noconflict/theme-eclipse'

const CreateTaskEditorMode: React.FC<{
  initialValues?: TaskFormData
  onSubmit: (data: TaskFormData) => void
}> = ({ onSubmit, initialValues }) => {
  const [t] = useTranslation()
  const navigate = useNavigate()
  const [val, setVal] = useState('')
  const [convertTaskDataToConfigFile] = useDmapiConverterTaskMutation()

  const handleEditorChange = (v: string) => setVal(v)
  const handleCancel = () => navigate('/migration/task')
  const handleSubmit = async () => {
    const res = await convertTaskDataToConfigFile({
      task_config_file: val,
    }).unwrap()
    onSubmit(res.task)
  }

  useEffect(() => {
    if (initialValues?.name) {
      convertTaskDataToConfigFile({ task: initialValues })
        .unwrap()
        .then(res => {
          setVal(res.task_config_file)
        })
    }
  }, [initialValues])

  return (
    <div className="h-full flex flex-col p-4">
      <div className="mb-4 flex flex-row-reverse">
        <Space>
          <Button onClick={handleCancel}>{t('cancel')}</Button>
          <Button type="primary" onClick={handleSubmit}>
            {t('submit')}
          </Button>
        </Space>
      </div>
      <AceEditor
        className="flex-1"
        mode="yaml"
        theme="eclipse"
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

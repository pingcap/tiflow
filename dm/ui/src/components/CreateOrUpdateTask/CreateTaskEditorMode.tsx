import React from 'react'

import { TaskFormData } from '~/models/task'

const CreateTaskEditorMode: React.FC<{
  initialValues?: TaskFormData
}> = ({ initialValues }) => {
  return (
    <div className="whitespace-pre">
      {JSON.stringify(initialValues, null, 2)}
    </div>
  )
}

export default CreateTaskEditorMode

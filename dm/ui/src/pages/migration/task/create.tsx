import React from 'react'
import { useTranslation } from 'react-i18next'

import { Breadcrumb } from '~/uikit'
import CreateTaskConfig from '~/components/CreateOrUpdateTask'

const CreateTask: React.FC = () => {
  const [t] = useTranslation()

  return (
    <div className="flex flex-col h-full overflow-y-auto">
      <div className="px-4 pt-4">
        <Breadcrumb>
          <Breadcrumb.Item>{t('migration')}</Breadcrumb.Item>
          <Breadcrumb.Item>{t('task list')}</Breadcrumb.Item>
          <Breadcrumb.Item>{t('create task')}</Breadcrumb.Item>
        </Breadcrumb>
      </div>

      <CreateTaskConfig className="flex-1" />
    </div>
  )
}

export default CreateTask

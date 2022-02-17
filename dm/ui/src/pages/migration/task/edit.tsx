import React from 'react'
import { useTranslation } from 'react-i18next'

import { useAppSelector } from '~/models'
import { Breadcrumb } from '~/uikit'
import CreateTaskConfig from '~/components/CreateOrUpdateTask'

const EditTask: React.FC = () => {
  const [t] = useTranslation()
  const data = useAppSelector(state => state.globals.preloadedTask)

  return (
    <div>
      <div className="px-4 pt-4">
        <Breadcrumb>
          <Breadcrumb.Item>{t('migration')}</Breadcrumb.Item>
          <Breadcrumb.Item>{t('task list')}</Breadcrumb.Item>
          <Breadcrumb.Item>{t('edit task')}</Breadcrumb.Item>
        </Breadcrumb>
      </div>

      <CreateTaskConfig data={data} />
    </div>
  )
}

export default EditTask

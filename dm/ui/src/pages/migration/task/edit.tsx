import React from 'react'
import { useTranslation } from 'react-i18next'
import { Link } from 'react-router-dom'

import { useAppSelector } from '~/models'
import { Breadcrumb } from '~/uikit'
import CreateTaskConfig from '~/components/CreateOrUpdateTask'

const EditTask: React.FC = () => {
  const [t] = useTranslation()
  const data = useAppSelector(state => state.globals.preloadedTask)

  return (
    <div className="flex flex-col h-full overflow-y-auto">
      <div className="px-4 pt-4">
        <Breadcrumb>
          <Breadcrumb.Item>{t('migration')}</Breadcrumb.Item>
          <Breadcrumb.Item>
            <Link to="/migration/task">{t('task list')}</Link>
          </Breadcrumb.Item>
          <Breadcrumb.Item>{t('edit task')}</Breadcrumb.Item>
        </Breadcrumb>
      </div>

      <CreateTaskConfig data={data} className="flex-1" />
    </div>
  )
}

export default EditTask

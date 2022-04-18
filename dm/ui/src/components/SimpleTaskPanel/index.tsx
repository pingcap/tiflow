import React from 'react'

import { DatabaseOutlined, FlagOutlined } from '~/uikit/icons'
import { calculateTaskStatus, Task } from '~/models/task'
import TaskUnitTag from '~/components/SimpleTaskPanel/TaskUnitTag'

const SimpleTaskPanel: React.FC<{
  task: Task
  preventClick?: boolean
}> = ({ task, preventClick }) => {
  const { status_list: statusList, name } = task
  return (
    <div
      className="flex-1 flex justify-between"
      onClick={e => {
        if (preventClick) {
          e.stopPropagation()
          e.preventDefault()
        }
      }}
    >
      <span>
        <DatabaseOutlined className="mr-2" />
        {name}
      </span>
      <span>
        <TaskUnitTag status={task.status_list} />
      </span>
      <span>
        <FlagOutlined className="mr-2" />
        {calculateTaskStatus(statusList)}
      </span>
    </div>
  )
}

export default SimpleTaskPanel

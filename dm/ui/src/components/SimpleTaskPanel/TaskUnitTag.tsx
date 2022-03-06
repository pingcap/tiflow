import React from 'react'

import { Tag } from '~/uikit/'
import { Task, TaskUnit } from '~/models/task'

const TaskUnitTag: React.FC<{
  status: Task['status_list']
}> = ({ status }) => {
  const syncCount = status?.filter(i => i.unit === TaskUnit.Sync).length
  const dumpCount = status?.filter(i => i.unit === TaskUnit.Dump).length
  const loadCount = status?.filter(i => i.unit === TaskUnit.Load).length
  return (
    <>
      <Tag color="cyan">{`Dump ${dumpCount}`}</Tag>
      <Tag color="blue">{`Load ${loadCount}`}</Tag>
      <Tag color="green">{`Sync ${syncCount}`}</Tag>
    </>
  )
}

export default TaskUnitTag

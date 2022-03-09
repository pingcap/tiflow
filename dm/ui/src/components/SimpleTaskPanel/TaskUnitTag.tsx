import React from 'react'

import { Tag } from '~/uikit/'
import { Task, TaskUnit } from '~/models/task'

const colors: { [index: string]: string } = {
  [TaskUnit.Load]: 'blue',
  [TaskUnit.Dump]: 'cyan',
  [TaskUnit.Sync]: 'green',
}

export const UnitTag: React.FC<{ unit: TaskUnit }> = ({ unit, children }) => {
  return <Tag color={colors[unit]}>{children}</Tag>
}

const TaskUnitTag: React.FC<{
  status: Task['status_list']
}> = ({ status }) => {
  const syncCount = status?.filter(i => i.unit === TaskUnit.Sync).length
  const dumpCount = status?.filter(i => i.unit === TaskUnit.Dump).length
  const loadCount = status?.filter(i => i.unit === TaskUnit.Load).length
  return (
    <>
      <UnitTag unit={TaskUnit.Dump}>{`Dump ${dumpCount}`}</UnitTag>
      <UnitTag unit={TaskUnit.Load}>{`Load ${loadCount}`}</UnitTag>
      <UnitTag unit={TaskUnit.Sync}>{`Sync ${syncCount}`}</UnitTag>
    </>
  )
}

export default TaskUnitTag

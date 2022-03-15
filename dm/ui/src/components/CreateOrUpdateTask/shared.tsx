import React from 'react'

import { TaskFormData } from '~/models/task'

export type StepCompnent = React.FC<{
  prev?: () => void
  next?: () => void
  submit?: (values: TaskFormData) => void
  initialValues?: TaskFormData
  isEditing?: boolean
}>

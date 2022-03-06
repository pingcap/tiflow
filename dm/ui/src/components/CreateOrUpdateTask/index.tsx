import React, { useState, useCallback, useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import { merge, omit } from 'lodash-es'
import { useNavigate, useLocation } from 'react-router-dom'

import { Card, Form, Steps, message } from '~/uikit'
import {
  OnDuplicateBehavior,
  Task,
  TaskFormData,
  TaskMode,
  useDmapiCreateTaskMutation,
  useDmapiUpdateTaskMutation,
} from '~/models/task'
import BasicInfo from '~/components/CreateOrUpdateTask/BasicInfo'
import SourceInfo from '~/components/CreateOrUpdateTask/SourceInfo'
import TargetInfo from '~/components/CreateOrUpdateTask/TargetInfo'
import EventFilters from '~/components/CreateOrUpdateTask/EventFilter'
import MigrateRule from '~/components/CreateOrUpdateTask/MigrateRule'
import CreateTaskEditorMode from '~/components/CreateOrUpdateTask/CreateTaskEditorMode'
import { isEmptyObject } from '~/utils/isEmptyObject'

const { Step } = Steps

const defaultValue = {
  name: '',
  task_mode: TaskMode.ALL,
  meta_schema: '',
  enhance_online_schema_change: true,
  on_duplicate: OnDuplicateBehavior.ERROR,
  target_config: {
    host: '',
    port: 3306,
    user: '',
    password: '',
  },
  source_config: {
    source_conf: [],
  },
  table_migrate_rule: [],
}

const stepComponents = [
  BasicInfo,
  SourceInfo,
  TargetInfo,
  EventFilters,
  MigrateRule,
]

const CreateTaskConfig: React.FC<{
  data?: Task | null
  className?: string
}> = ({ data, className }) => {
  const [t] = useTranslation()
  const loc = useLocation()
  const navigate = useNavigate()
  const [loading, setLoading] = useState(true)
  const [currentStep, setCurrentStep] = useState(0)
  const [taskData, setTaskData] = useState<TaskFormData>(defaultValue)
  const [createTask] = useDmapiCreateTaskMutation()
  const [updateTask] = useDmapiUpdateTaskMutation()
  const desciptions = [
    t('create task basic info desc'),
    t('create task source info desc'),
    t('create task target info desc'),
    t('create task event filters desc'),
    t('create task migrate rule desc'),
  ]

  const goNextStep = useCallback(() => {
    setCurrentStep(c => c + 1)
  }, [])
  const goPrevStep = useCallback(() => {
    setCurrentStep(c => c - 1)
  }, [])

  const handleSubmit = (taskData: TaskFormData) => {
    const isEditing = Boolean(data)
    const key = 'createTask-' + Date.now()
    message.loading({ content: t('requesting'), key })
    const payload = { ...taskData }
    if ('binlog_filter_rule_array' in payload) {
      delete payload.binlog_filter_rule_array
    }
    if (isEmptyObject(payload.target_config.security)) {
      delete payload.target_config.security
    }
    const handler = isEditing ? updateTask : createTask
    handler({ task: payload as Task })
      .unwrap()
      .then(() => {
        message.success({ content: t('request success'), key })
        navigate('#')
      })
      .catch(() => {
        message.destroy(key)
      })
  }

  const getStepComponent = () => {
    const Comp = stepComponents[currentStep]
    return React.cloneElement(<Comp />, {
      prev: goPrevStep,
      next: goNextStep,
      submit: handleSubmit,
      initialValues: taskData,
    })
  }

  useEffect(() => {
    if (data) {
      setTaskData(
        merge({}, defaultValue, omit(data, 'status_list'), {
          binlog_filter_rule_array: Object.entries(
            data?.binlog_filter_rule ?? {}
          ).map(([name, value]) => ({ name, ...value })),
        })
      )
    }
    setLoading(false)
  }, [data])

  if (loc.hash === '#configFile') {
    return (
      <div className={className}>
        <CreateTaskEditorMode
          initialValues={taskData}
          onSubmit={data => handleSubmit(data)}
        />
      </div>
    )
  }

  return (
    <Card className="!m-4" loading={loading}>
      <div className="mb-8 p-4 rounded bg-white border-1 border-gray-300 border-dashed whitespace-pre-line">
        {desciptions[currentStep]}
      </div>

      <Steps current={currentStep}>
        <Step title={t('basic info')} />
        <Step title={t('source info')} />
        <Step title={t('target info')} />
        <Step title={t('event filter')} />
        <Step title={t('migrate rules')} />
      </Steps>
      <div className="mt-8">
        <Form.Provider
          onFormFinish={(formName, { values }) => {
            const nextTaskData = merge({}, taskData, values)
            setTaskData(nextTaskData)

            if (currentStep === stepComponents.length - 1) {
              handleSubmit(nextTaskData)
            } else {
              goNextStep()
            }
          }}
        >
          {getStepComponent()}
        </Form.Provider>
      </div>
    </Card>
  )
}

export default CreateTaskConfig

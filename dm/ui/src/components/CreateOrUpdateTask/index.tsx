import React, { useState, useCallback, useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import { omit } from 'lodash-es'
import { useNavigate, useLocation } from 'react-router-dom'

import { Card, Form, Steps, message } from '~/uikit'
import {
  OnDuplicateBehavior,
  Task,
  TaskFormData,
  TaskMode,
  TaskShardMode,
  useDmapiCreateTaskMutation,
  useDmapiStartTaskMutation,
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

const defaultValue: Partial<TaskFormData> = {
  name: '',
  task_mode: TaskMode.ALL,
  shard_mode: TaskShardMode.PESSIMISTIC,
  meta_schema: '',
  enhance_online_schema_change: true,
  on_duplicate: OnDuplicateBehavior.REPLACE,
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
  const [taskData, setTaskData] = useState<TaskFormData>(
    defaultValue as TaskFormData
  )
  const [createTask] = useDmapiCreateTaskMutation()
  const [updateTask] = useDmapiUpdateTaskMutation()
  const [startTask] = useDmapiStartTaskMutation()
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

  const handleSubmit = async (taskData: TaskFormData) => {
    const isEditing = Boolean(data)
    const payload = { ...taskData }
    const startAfterSaved = payload.start_after_saved
    if (payload.shard_mode === TaskShardMode.NONE) {
      delete payload.shard_mode
    }
    if ('binlog_filter_rule_array' in payload) {
      delete payload.binlog_filter_rule_array
    }
    if (isEmptyObject(payload.target_config.security)) {
      delete payload.target_config.security
    }
    if ('start_after_saved' in payload) {
      delete payload.start_after_saved
    }
    if (payload.table_migrate_rule.length > 0) {
      payload.table_migrate_rule.forEach(i =>
        isEmptyObject(i.target) ? delete i.target : null
      )
    }
    const handler = isEditing ? updateTask : createTask
    const key = 'createTask-' + Date.now()
    message.loading({ content: t('requesting'), key })
    try {
      const data = await handler({ task: payload as Task }).unwrap()
      if (startAfterSaved) {
        await startTask({ taskName: payload.name }).unwrap()
      }
      if (data.check_result) {
        message.success({ content: data.check_result, key })
      } else {
        message.success({ content: t('request success'), key })
      }
      navigate('/migration/task')
    } catch (e) {
      message.destroy(key)
    }
  }

  const getStepComponent = () => {
    const Comp = stepComponents[currentStep]
    return React.cloneElement(<Comp />, {
      prev: goPrevStep,
      next: goNextStep,
      submit: handleSubmit,
      initialValues: taskData,
      isEditing: Boolean(data),
    })
  }

  useEffect(() => {
    if (data) {
      setTaskData({
        ...omit(data, 'status_list'),
        binlog_filter_rule_array: Object.entries(
          data?.binlog_filter_rule ?? {}
        ).map(([name, value]) => ({ name, ...value })),
      })
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
            const nextTaskData = { ...taskData, ...values }
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

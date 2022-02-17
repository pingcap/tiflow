import React, { useState, useCallback, useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import { merge } from 'lodash'
import { useNavigate, useLocation } from 'react-router-dom'

import { Card, Form, Steps, message } from '~/uikit'
import {
  OnDuplicateBehavior,
  Task,
  TaskFormData,
  TaskMode,
  useDmapiStartTaskMutation,
} from '~/models/task'
import BasicInfo from '~/components/CreateOrUpdateTask/BasicInfo'
import SourceInfo from '~/components/CreateOrUpdateTask/SourceInfo'
import TargetInfo from '~/components/CreateOrUpdateTask/TargetInfo'
import EventFilters from '~/components/CreateOrUpdateTask/EventFilter'
import MigrateRule from '~/components/CreateOrUpdateTask/MigrateRule'
import CreateTaskEditorMode from '~/components/CreateOrUpdateTask/CreateTaskEditorMode'

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
}> = ({ data }) => {
  const [t] = useTranslation()
  const loc = useLocation()
  const navigate = useNavigate()
  const [loading, setLoading] = useState(true)
  const [currentStep, setCurrentStep] = useState(0)
  const [taskData, setTaskData] = useState<TaskFormData>(defaultValue)
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

  const handleSubmit = useCallback(
    taskData => {
      const key = 'createTaskConfig-' + Date.now()
      message.loading({ content: t('saving'), key })
      const payload = { ...taskData }
      delete payload.binlog_filter_rule_array
      if (
        Object.values(payload.target_config.security).filter(Boolean).length ===
        0
      ) {
        delete payload.target_config.security
      }
      startTask({
        task: payload as Task,
        remove_meta: true,
      })
        .unwrap()
        .then(() => {
          message.success({ content: t('saved'), key })
          navigate('#')
        })
        .catch(() => {
          message.destroy(key)
        })
    },
    [taskData]
  )

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
        merge({}, defaultValue, data, {
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
      <Card className="!m-4" loading={loading}>
        <CreateTaskEditorMode initialValues={taskData} />
      </Card>
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

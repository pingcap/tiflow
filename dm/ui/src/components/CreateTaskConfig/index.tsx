import React, { useState, useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { merge } from 'lodash'
import { useNavigate } from 'react-router-dom'

import { Card, Form, Steps, message } from '~/uikit'
import {
  OnDuplicateBehavior,
  Task,
  TaskFormData,
  TaskMode,
} from '~/models/task'
import BasicInfo from '~/components/CreateTaskConfig/BasicInfo'
import SourceInfo from '~/components/CreateTaskConfig/SourceInfo'
import TargetInfo from '~/components/CreateTaskConfig/TargetInfo'
import EventFilters from '~/components/CreateTaskConfig/EventFilter'
import MigrateRule from '~/components/CreateTaskConfig/MigrateRule'
import { useDmapiCreateTaskConfigMutation } from '~/models/taskConfig'

const { Step } = Steps

const defaultValue = {
  name: '',
  task_mode: TaskMode.ALL,
  meta_schema: '',
  enhance_online_schema_change: false,
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

const CreateTaskConfig: React.FC = () => {
  const [t] = useTranslation()
  const navigate = useNavigate()
  const [currentStep, setCurrentStep] = useState(0)
  const [taskData, setTaskData] = useState<TaskFormData>(defaultValue)
  const [createTaskConfig] = useDmapiCreateTaskConfigMutation()

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
      createTaskConfig(payload as Task)
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

  return (
    <Card className="!m-4">
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

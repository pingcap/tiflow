import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { clamp } from 'lodash'
import { useSafeState } from 'ahooks'
import QueueAnim from 'rc-queue-anim'
import { PrismAsyncLight as SyntaxHighlighter } from 'react-syntax-highlighter'
import { ghcolors } from 'react-syntax-highlighter/dist/esm/styles/prism'
import json from 'react-syntax-highlighter/dist/esm/languages/prism/json'

import { Button, Radio, Checkbox, Steps, Alert, StepProps } from '~/uikit'
import {
  BatchImportTaskConfigResponse,
  useDmapiBatchImportTaskConfigMutation,
} from '~/models/taskConfig'
import { sleep } from '~/utils/sleep'

SyntaxHighlighter.registerLanguage('json', json)

const { Step } = Steps

const BlinkingText: React.FC<{ text: string; active: boolean }> = ({
  text,
  active,
}) => {
  const [count, setCount] = useSafeState(1)
  let timer = useRef<number>()

  useEffect(() => {
    if (active) {
      timer.current = window.setInterval(() => setCount(c => (c + 1) % 4), 300)
    }
    return () => {
      if (timer.current) {
        clearInterval(timer.current)
      }
    }
  }, [active])

  if (!active) {
    return <span>{text}</span>
  }
  return <span>{text + '.'.repeat(count % 4)}</span>
}

const BatchImportTaskConfig: React.FC = () => {
  const [t] = useTranslation()
  const [percent, setPercent] = useSafeState(0)
  const [error, setError] = useState<any>()
  const [importResult, setImportResult] =
    useState<BatchImportTaskConfigResponse>()
  const [currentStep, setCurrentStep] = useSafeState(-1)
  const [overwrite, setOverwrite] = useState(false)
  const [batchImportTaskConfig] = useDmapiBatchImportTaskConfigMutation()
  const timerId = useRef<number>()
  const stopTimer = useRef(false)

  const calcStepStatus = (thisStep: number): Pick<StepProps, 'status'> => {
    if (currentStep === thisStep) {
      if (currentStep === 1 && error) {
        return { status: 'error' }
      }
      return { status: 'process' }
    }
    if (currentStep > thisStep) return { status: 'finish' }
    return { status: 'wait' }
  }

  const handleStartImport = () => {
    setCurrentStep(0)
    handleStartPlaceboProgress()
  }

  const increment = () => {
    timerId.current = window.setTimeout(increment, Math.random() * 100)

    setPercent(percent => {
      if (percent >= 100) {
        return 100
      }
      let n = percent
      let amount: number
      if (n >= 0 && n < 20) {
        amount = 1
      } else if (n >= 20 && n < 50) {
        amount = 4
      } else if (n >= 50 && n < 80) {
        amount = 2
      } else if (n >= 80 && n < 99) {
        amount = 5
      } else {
        amount = 0
      }
      n = clamp(n + amount, 0, 80) // clamp here to make sure awalys 0 <= n <= 80
      return n
    })
  }

  const waitLittleWhileThenGoToNextStep = async () => {
    await sleep(600) // everytime we wait for little while before continue to next step for better ux
    setCurrentStep(c => c + 1)
  }

  const clearTimer = () => {
    stopTimer.current = true
    clearTimeout(timerId.current)
  }

  const handleStartPlaceboProgress = useCallback(async () => {
    // phase 1
    increment()
    await sleep(1000 * Math.random() + 300)
    setPercent(100)
    await waitLittleWhileThenGoToNextStep()

    // phase 2
    setPercent(0)
    try {
      await sleep(3000)
      const res = await batchImportTaskConfig({ overwrite }).unwrap()
      setImportResult(res)
    } catch (e: any) {
      setError(e)
      clearTimer()
      await sleep(600)
      setCurrentStep(4)
      return
    }

    setPercent(100)
    await waitLittleWhileThenGoToNextStep()

    // phase 3
    setPercent(0)
    await sleep(1000)
    clearTimer()
    setPercent(100)

    await waitLittleWhileThenGoToNextStep()
  }, [percent, error])

  useEffect(() => {
    return () => {
      if (timerId.current) {
        clearTimeout(timerId.current)
      }
    }
  }, [])

  return (
    <div className="flex flex-col justify-center items-center">
      {currentStep === -1 && (
        <>
          <div className="p-4 w-300px rounded border border-1 border-solid border-gray-200">
            <Radio defaultChecked className="font-bold">
              {t('import runtime task config')}
            </Radio>
            <p className="pl-6 text-gray-400">
              {t('import all task config of dm cluster to web ui')}
            </p>
          </div>
          <div className="p-4 w-300px">
            <Checkbox onChange={e => setOverwrite(e.target.checked)}>
              {t('overwrite exist task config')}
            </Checkbox>
          </div>

          <div className="p-4">
            <Button onClick={handleStartImport}>{t('start import')}</Button>
          </div>
        </>
      )}

      {currentStep >= 0 && currentStep < 3 && (
        <Steps direction="vertical" current={currentStep} percent={percent}>
          <Step
            {...calcStepStatus(0)}
            title={
              <BlinkingText
                text={t('reading task list')}
                active={currentStep === 0}
              />
            }
          />
          <Step
            {...calcStepStatus(1)}
            title={
              <BlinkingText
                text={t('processing imports')}
                active={currentStep === 1}
              />
            }
          />
          <Step
            {...calcStepStatus(2)}
            title={
              <BlinkingText
                text={t('collecting import results')}
                active={currentStep === 2}
              />
            }
          />
        </Steps>
      )}

      <QueueAnim className="w-full">
        {currentStep >= 3
          ? [
              <div key="1">
                {error ? (
                  <div className="p-4">
                    <Alert
                      showIcon
                      message={t('import failed')}
                      description={
                        error?.data?.error_msg ??
                        t('somthing went wrong with the server')
                      }
                      type="error"
                    />
                  </div>
                ) : (
                  <div className="p-4 max-h-800px overflow-auto">
                    <Alert
                      showIcon
                      message={t(
                        'import finished with {{success_count}} success and {{fail_count}} failures',
                        {
                          success_count:
                            importResult?.success_task_list?.length ?? 0,
                          fail_count:
                            importResult?.failed_task_list?.length ?? 0,
                        }
                      )}
                      description={t('please check the log')}
                      type="success"
                    />

                    <SyntaxHighlighter style={ghcolors} language="json">
                      {importResult?.failed_task_list
                        ? JSON.stringify(importResult.failed_task_list, null, 2)
                        : 'null'}
                    </SyntaxHighlighter>
                  </div>
                )}
              </div>,
            ]
          : null}
      </QueueAnim>
    </div>
  )
}

export default BatchImportTaskConfig

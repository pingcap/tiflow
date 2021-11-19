import React, { useState, createContext } from 'react'

import StartStep from './components/StartStep'
import NamingStep from './components/NamingStep'
import InstancesStep from './components/InstancesStep'
import MigrateStep from './components/MigrateStep'
import UploadStep from './components/UploadStep'
import DmLayout from './components/Layout'
import { hot } from 'react-hot-loader/root'
import { ITaskInfo, IInstances, ISourceConfig, IFullSchemas } from './types'
import { genDefSourceInstance, genDefDbConfig } from './utils/config-util'

enum Steps {
  Start,
  Naming,
  ConfigInstances,
  Migrate,
  Upload
}

export const EditContext = createContext(false)

function App() {
  const [step, setStep] = useState<Steps>(Steps.Start)
  const [edit, setEdit] = useState(false)

  const [taskInfo, setTaskInfo] = useState<ITaskInfo>({
    taskName: '',
    taskMode: 'incremental'
  })
  const [instances, setInstances] = useState<IInstances>(() => ({
    targetInstance: genDefDbConfig(4000),
    sourceInstances: [genDefSourceInstance()]
  }))
  const [sourceConfig, setSourceConfig] = useState<ISourceConfig>({
    sourceInstances: {},
    sourceSchemas: {},
    allTables: {}
  })
  const [targetSchemas, setTargetSchemas] = useState<IFullSchemas>({})

  function onNewRule() {
    setStep(Steps.Naming)
    setEdit(false)

    setTargetSchemas({})
  }

  function onEditRule() {
    setStep(Steps.Upload)
    setEdit(true)
  }

  function onParseFinalConfig(parsedConfig: any) {
    setTaskInfo(parsedConfig.taskInfo)
    setInstances(parsedConfig.instances)
    setSourceConfig({
      sourceInstances: parsedConfig.sourceFullInstances,
      sourceSchemas: parsedConfig.sourceSchemas,
      allTables: parsedConfig.allTables
    })
    setTargetSchemas(parsedConfig.targetSchemas)
  }

  function renderContent() {
    switch (step) {
      case Steps.Start:
        return <StartStep onNewRule={onNewRule} onEditRule={onEditRule} />
      case Steps.Naming:
        return (
          <NamingStep
            taskInfo={taskInfo}
            onData={taskInfo => setTaskInfo(taskInfo)}
            onNext={() => setStep(edit ? Steps.Migrate : Steps.ConfigInstances)}
            onPrev={() => setStep(Steps.Start)}
          />
        )
      case Steps.ConfigInstances:
        return (
          <InstancesStep
            showBinlog={taskInfo.taskMode === 'incremental'}
            instances={instances}
            onData={setInstances}
            onFetchSourceConfig={setSourceConfig}
            onNext={() => setStep(Steps.Migrate)}
            onPrev={() => setStep(Steps.Naming)}
          />
        )
      case Steps.Migrate:
        return (
          <MigrateStep
            taskInfo={taskInfo}
            instancesConfig={instances}
            sourceConfig={sourceConfig}
            targetSchemas={targetSchemas}
            onNext={() => setStep(Steps.Start)}
            onPrev={() => setStep(edit ? Steps.Naming : Steps.ConfigInstances)}
          />
        )
      case Steps.Upload:
        return (
          <UploadStep
            onData={onParseFinalConfig}
            onNext={() => setStep(Steps.Naming)}
            onPrev={() => setStep(Steps.Start)}
          />
        )
    }
  }

  return (
    <EditContext.Provider value={edit}>
      <DmLayout>{renderContent()}</DmLayout>
    </EditContext.Provider>
  )
}

export default process.env.NODE_ENV === 'development' ? hot(App) : App

export interface IPageAction<T> {
  onNext: () => void
  onPrev: () => void
  onData?: (t: T) => void
}

/////

export type ITaskMode = 'full' | 'incremental' | 'all'

export interface ITaskInfo {
  taskName: string
  taskMode: ITaskMode
}

/////

export interface IDatabase {
  host: string
  port: number
  user: string
  password: string
}

export interface IBinlogMeta {
  binlogName: string
  binlogPos: number
}

export type ITargetInstance = IDatabase

export interface ISourceInstance {
  sourceId: string
  binlogMeta: IBinlogMeta
  dbConfig: IDatabase

  uuid: number // used to be key of the instance component
}

export interface IInstances {
  targetInstance: ITargetInstance
  sourceInstances: ISourceInstance[]
}

/////

export interface IOriginSchema {
  schema: string
  tables: string[]
}

export interface IOriginSourceInstance {
  sourceId: string
  schemas: IOriginSchema[]
}

export interface IKey {
  key: string
}

export interface IFullTable extends IKey {
  type: 'table' | 'mergedTable'

  sourceId: string
  schema: string
  table: string

  newName: string
  // if table stay in the source schema, the parentKey will always be empty string
  // it only has value when move to target schema, and parentKey points to schema or mergedTable
  parentKey: string
  mergedTables?: string[]

  // TODO: rename to binlogFilters
  filters: string[]
}

export interface IFullSchema extends IKey {
  type: 'schema'

  sourceId: string
  schema: string
  tables: string[] // tables keys

  newName: string

  // TODO: rename to binlogFilters
  filters: string[]
}

export interface IFullInstance extends IKey {
  type: 'instance'

  sourceId: string
  schemas: string[] // schemas keys
}

export interface IFullInstances {
  [key: string]: IFullInstance
}

export interface IFullSchemas {
  [key: string]: IFullSchema
}

export interface IFullTables {
  [key: string]: IFullTable
}

export interface ISourceConfig {
  sourceInstances: IFullInstances
  sourceSchemas: IFullSchemas
  allTables: IFullTables
}

/////

export interface IRoute {
  'schema-pattern': string
  'target-schema': string
  'table-pattern'?: string
  'target-table'?: string
}

export interface IRoutes {
  [key: string]: IRoute
}

export interface IFilter {
  'schema-pattern': string
  'table-pattern'?: string
  events: string[]
  action: 'Ignore'
}

export interface IFilters {
  [key: string]: IFilter
}

// BAList: Block Allow List
export interface IBATable {
  'db-name': string
  'tbl-name': string
}

export interface IBASchema {
  'do-tables': IBATable[]
  'ignore-tables': IBATable[]
}

export interface IBAList {
  [key: string]: IBASchema
}

export interface IMySQLInstance {
  'source-id': string
  meta: {
    'binlog-name': string
    'binlog-pos': number
  }
  'db-config': IDatabase
}

export interface IFinalConfig {
  name: string
  'task-mode': ITaskMode

  'target-database': ITargetInstance
  'mysql-instances': IMySQLInstance[]

  routes: IRoutes
  filters: IFilters
  'block-allow-list': IBAList
}

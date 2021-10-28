import {
  IFullSchemas,
  IRoutes,
  IFullTables,
  IOriginSourceInstance,
  ISourceConfig,
  IFullInstances,
  IFullInstance,
  IFullSchema,
  IFullTable,
  ISourceInstance,
  IFilters,
  IBAList,
  ITaskInfo,
  IInstances,
  IFinalConfig,
  ITargetInstance,
  IBATable,
  IRoute,
  IDatabase
} from '../types'

/////////////////////////

export function genDefDbConfig(port: number = 3306): IDatabase {
  return {
    host: '192.168.0.1',
    port,
    user: 'root',
    password: ''
  }
}

let sourceInstanceId = 0
export function genDefSourceInstance(): ISourceInstance {
  sourceInstanceId++
  return {
    sourceId: `replica-${sourceInstanceId}`,
    binlogMeta: {
      binlogName: 'mysql-bin.0000001',
      binlogPos: 4
    },
    dbConfig: genDefDbConfig(),

    uuid: Date.now() + Math.floor(Math.random() * 10000)
  }
}

/////////////////////////

// const mockedOriginSourceInstances: IOriginSourceInstance[] = [
//   {
//     sourceId: 'source-1',
//     schemas: [
//       {
//         schema: 'schema_1',
//         tables: ['table_1', 'table_2']
//       },
//       {
//         schema: 'schema_2',
//         tables: ['table_3', 'table_4']
//       }
//     ]
//   },
//   {
//     sourceId: 'source-2',
//     schemas: [
//       {
//         schema: 'schema_1',
//         tables: ['table_1', 'table_2']
//       },
//       {
//         schema: 'schema_2',
//         tables: []
//       }
//     ]
//   },
//   {
//     sourceId: 'source-3',
//     schemas: [
//       {
//         schema: 'schema_1',
//         tables: ['table_1', 'table_2']
//       }
//     ]
//   }
// ]

export function convertSourceInstances(
  originSourceTables: IOriginSourceInstance[]
): ISourceConfig {
  const sourceInstances: IFullInstances = {}
  const sourceSchemas: IFullSchemas = {}
  const allTables: IFullTables = {}

  originSourceTables.forEach(instance => {
    const fullInstance: IFullInstance = {
      type: 'instance',
      key: instance.sourceId,
      sourceId: instance.sourceId,
      schemas: instance.schemas.map(
        item => `${instance.sourceId}:${item.schema}`
      )
    }
    sourceInstances[fullInstance.key] = fullInstance

    instance.schemas.forEach(schema => {
      const fullSchema: IFullSchema = {
        type: 'schema',
        key: `${instance.sourceId}:${schema.schema}`,
        sourceId: instance.sourceId,
        schema: schema.schema,
        tables: schema.tables.map(
          item => `${instance.sourceId}:${schema.schema}:${item}`
        ),
        newName: schema.schema,
        filters: []
      }
      sourceSchemas[fullSchema.key] = fullSchema

      schema.tables.forEach(table => {
        const fullTable: IFullTable = {
          type: 'table',
          key: `${instance.sourceId}:${schema.schema}:${table}`,
          sourceId: instance.sourceId,
          schema: schema.schema,
          table: table,
          newName: table,
          parentKey: '',
          filters: []
        }
        allTables[fullTable.key] = fullTable
      })
    })
  })

  return { sourceInstances, sourceSchemas, allTables }
}

// const mockedSourceConfig = convertSourceInstances(mockedOriginSourceInstances)

/////////////////////////

// "all": "all dml", "all ddl"
// "all dml": "insert", "update","delete"
// "all ddl": "create database", "drop database", "create table", // only for db
//            "drop table", "truncate table", "rename table", "alter table", "create index", "drop index" // for both: db and table
export const DDL_FOR_SCHEMA: string[] = [
  'create database',
  'drop database',
  'create table'
]
export const DDL_FOR_TABLE: string[] = [
  'drop table',
  'truncate table',
  'rename table',
  'alter table',
  'create index',
  'drop index'
]
export const ALL_DDL: string[] = DDL_FOR_SCHEMA.concat(DDL_FOR_TABLE)
export const ALL_DML: string[] = ['insert', 'update', 'delete']

// ['all', 'all ddl', 'all dml', 'insert' ...] => ['all']
// ['all dml', 'insert' ..., 'drop table', ...] => ['all dml', 'drop table', ...]
// ['all ddl', 'drop table', ..., 'insert', ...] => ['all ddl', 'insert', ...]
function genFinalFilters(oriFilters: string[], forTable: boolean): string[] {
  let finalFilters: string[] = []
  if (oriFilters.includes('all')) {
    // if include 'all', means include 'all', 'all ddl', 'all dml' ...
    finalFilters = ['all']
    // else
    // not include 'all', but maybe include 'all ddl' or 'all dml'
    // can't include 'all ddl' and 'all dml' both
  } else if (oriFilters.includes('all ddl')) {
    finalFilters = oriFilters.filter(item => !ALL_DDL.includes(item))
  } else if (oriFilters.includes('all dml')) {
    finalFilters = oriFilters.filter(item => !ALL_DML.includes(item))
  } else {
    finalFilters = [...oriFilters]
  }

  if (forTable) {
    finalFilters = finalFilters.filter(item => !DDL_FOR_SCHEMA.includes(item))
  }

  return finalFilters
}

/////////////////////////

function instancesRulesCounter() {
  // 闭包
  const instancesRulesCnt: { [key: string]: number } = {}

  function genRuleKey(
    instanceId: string,
    ruleType: 'route_rules' | 'filter' | 'ba_list' = 'route_rules'
  ) {
    const curCnt = instancesRulesCnt[instanceId] || 0
    instancesRulesCnt[instanceId] = curCnt + 1
    return `${instanceId}.${ruleType}.${curCnt + 1}`
  }

  return { genRuleKey }
}

export function genRoutesConfig(
  targetSchemas: IFullSchemas,
  allTables: IFullTables
): IRoutes {
  // 生成 routes config
  // 依次遍历 targetSchemas 和相应的 tables

  const routeRulesCounter = instancesRulesCounter()
  const routes: IRoutes = {}

  Object.keys(targetSchemas).forEach(schemaKey => {
    const curSchema = targetSchemas[schemaKey]
    if (curSchema.sourceId !== 'new') {
      const routeKey = routeRulesCounter.genRuleKey(curSchema.sourceId)
      routes[routeKey] = {
        'schema-pattern': curSchema.schema,
        'target-schema': curSchema.newName
      }
    }

    // 后面的逻辑只跟 curSchema.newName 有关系
    curSchema.tables.forEach(tableKey => {
      const curTable = allTables[tableKey]
      if (curTable.type === 'table') {
        const routeKey = routeRulesCounter.genRuleKey(curTable.sourceId)
        // table 的 schema-pattern 是 curTable.schema，而不是 curSchema.schema
        routes[routeKey] = {
          'schema-pattern': curTable.schema,
          'target-schema': curSchema.newName,
          'table-pattern': curTable.table,
          'target-table': curTable.newName
        }
      } else if (curTable.type === 'mergedTable') {
        curTable.mergedTables!.forEach(tbKey => {
          const bottomTable = allTables[tbKey]
          const routeKey = routeRulesCounter.genRuleKey(bottomTable.sourceId)
          // 对于合并表来说，子表的 target-table 并不是子表自身的 newName，而是合并表的 newName
          // 所有对于合并表的子表来说，重命名毫无意义? 好像也是有点意义，拖动以后不需要再重命名
          routes[routeKey] = {
            'schema-pattern': bottomTable.schema,
            'target-schema': curSchema.newName,
            'table-pattern': bottomTable.table,
            'target-table': curTable.newName
          }
        })
      }
    })
  })
  return routes
}

export function genFiltersConfig(
  sourceSchemas: IFullSchemas,
  allTables: IFullTables
): IFilters {
  // 生成 filters config
  // 分两步走
  // 1. 生成 tables 的 filters config: 从 allTables 中过滤出移到右边的 table 且 filters 值不为空
  // 2. 生成 schema 的 filters config: 从 allTables 中过滤出移到右边的 table，且 type 是 table (不能是 mergedTable)，从中得到它们的 schemaKey
  const filterRulesCounter = instancesRulesCounter()
  const filters: IFilters = {}

  const hasFiltersTables: IFullTable[] = Object.keys(allTables)
    .map(tableKey => allTables[tableKey])
    .filter(table => table.filters.length > 0 && table.parentKey !== '')
  hasFiltersTables.forEach(table => {
    const filterKey = filterRulesCounter.genRuleKey(table.sourceId, 'filter')
    filters[filterKey] = {
      'schema-pattern': table.schema,
      'table-pattern': table.table,
      events: genFinalFilters(table.filters, true),
      action: 'Ignore'
    }
  })

  const schemaKeys: string[] = Object.keys(allTables)
    .map(tableKey => allTables[tableKey])
    .filter(table => table.parentKey !== '' && table.type === 'table')
    .map(table => `${table.sourceId}:${table.schema}`)
  const schemaKeysSet = new Set(schemaKeys)
  schemaKeysSet.forEach(schemaKey => {
    const curSchema = sourceSchemas[schemaKey]
    if (curSchema.filters.length > 0) {
      const filterKey = filterRulesCounter.genRuleKey(
        curSchema.sourceId,
        'filter'
      )
      filters[filterKey] = {
        'schema-pattern': curSchema.schema,
        events: genFinalFilters(curSchema.filters, false),
        action: 'Ignore'
      }
    }
  })
  return filters
}

export function genBlockAllowList(
  allTables: IFullTables,
  autoSycnUpstream: boolean
): IBAList {
  const baList: IBAList = {}
  const tables = Object.keys(allTables)
    .map(tableKey => allTables[tableKey])
    .filter(table => table.type === 'table')
  tables.forEach(table => {
    const baListKey = `${table.sourceId}.ba_list.1`
    if (baList[baListKey] === undefined) {
      baList[baListKey] = { 'do-tables': [], 'ignore-tables': [] }
    }
    const baType: 'do-tables' | 'ignore-tables' =
      table.parentKey !== '' ? 'do-tables' : 'ignore-tables'
    if (autoSycnUpstream && baType === 'do-tables') {
      return
    }
    baList[baListKey][baType].push({
      'db-name': table.schema,
      'tbl-name': table.table
    })
  })
  return baList
}

export function genFinalConfig(
  taskInfo: ITaskInfo,
  instancesConfig: IInstances,
  sourceSchemas: IFullSchemas,
  targetSchemas: IFullSchemas,
  allTables: IFullTables,
  autoSycnUpstream: boolean
) {
  const routes: IRoutes = genRoutesConfig(targetSchemas, allTables)
  const filters: IFilters = genFiltersConfig(sourceSchemas, allTables)
  const baList: IBAList = genBlockAllowList(allTables, autoSycnUpstream)

  const finalConfig = {
    name: taskInfo.taskName,
    'task-mode': taskInfo.taskMode,

    'target-database': instancesConfig.targetInstance,
    'mysql-instances': instancesConfig.sourceInstances.map(inst => ({
      'source-id': inst.sourceId,
      meta: {
        'binlog-name': inst.binlogMeta.binlogName,
        'binlog-pos': inst.binlogMeta.binlogPos
      },
      'db-config': inst.dbConfig
    })),

    routes,
    filters,
    'block-allow-list': baList
  }
  console.log(finalConfig)
  return finalConfig
}

//////////////////////////////////////////////////////////////////////////////////

function genDefFullInstance(sourceId: string): IFullInstance {
  return {
    type: 'instance',
    key: sourceId,
    sourceId,
    schemas: []
  }
}

function genDefFullSchema(
  sourceId: string,
  dbName: string,
  newTargetSchema: boolean = false
): IFullSchema {
  const schemaKey = `${sourceId}:${dbName}`
  return {
    type: 'schema',
    key: schemaKey,
    sourceId,
    schema: newTargetSchema ? 'newdatabase' : dbName,
    tables: [],
    newName: dbName,
    filters: []
  }
}

function genDefFullTable(
  sourceId: string,
  dbName: string,
  tblName: string
): IFullTable {
  const tableKey = `${sourceId}:${dbName}:${tblName}`
  return {
    type: 'table',
    key: tableKey,

    sourceId,
    schema: dbName,
    table: tblName,

    newName: tblName,
    parentKey: '',
    mergedTables: [],
    filters: []
  }
}

export function parseFinalConfig(finalConfig: IFinalConfig) {
  const taskInfo = {
    taskName: finalConfig.name,
    taskMode: finalConfig['task-mode']
  }

  /////

  const targetInstance: ITargetInstance = finalConfig['target-database']

  let uuid = 0
  const sourceInstances: ISourceInstance[] = finalConfig['mysql-instances'].map(
    inst => ({
      sourceId: inst['source-id'],
      binlogMeta: {
        binlogName: inst.meta['binlog-name'],
        binlogPos: inst.meta['binlog-pos']
      },
      dbConfig: inst['db-config'],
      uuid: uuid++
    })
  )

  const instances: IInstances = { targetInstance, sourceInstances }

  /////

  function parseBAListItem(
    instance: IFullInstance,
    table: IBATable,
    migrated: boolean
  ) {
    const sourceId = instance.sourceId
    const dbName = table['db-name']
    const tblName = table['tbl-name']

    const schemaKey = `${sourceId}:${dbName}`
    const schema =
      sourceSchemas[schemaKey] || genDefFullSchema(sourceId, dbName)
    sourceSchemas[schemaKey] = schema

    const fullTable: IFullTable = genDefFullTable(sourceId, dbName, tblName)
    allTables[fullTable.key] = fullTable

    if (!instance.schemas.includes(schema.key)) {
      instance.schemas.push(schema.key)
    }

    if (migrated) {
      // 右移到下游的表
      fullTable.parentKey = 'unknown'
    } else {
      // 留在上游的表
      schema.tables.push(fullTable.key)
    }
  }

  /////

  const sourceFullInstances: IFullInstances = {}
  const sourceSchemas: IFullSchemas = {}
  const allTables: IFullTables = {}

  // 第一步：从 block-allow-lists 中还原得到 sourceFullInstances, sourceSchemas, allTables
  const baList: IBAList = finalConfig['block-allow-list']
  Object.keys(baList).forEach(baListKey => {
    // baListKey => "replica-1.ba_list.1"
    const sourceId: string = baListKey.split('.')[0]
    const sourceFullInstance: IFullInstance = genDefFullInstance(sourceId)
    sourceFullInstances[sourceId] = sourceFullInstance

    const doTables: IBATable[] = baList[baListKey]['do-tables']
    const ignoreTables: IBATable[] = baList[baListKey]['ignore-tables']
    doTables.forEach(table => parseBAListItem(sourceFullInstance, table, true))
    ignoreTables.forEach(table =>
      parseBAListItem(sourceFullInstance, table, false)
    )
  })

  // 第二步：从 routes 中还原得到 targetSchemas，更新 allTables parentKey 为 unknown 的 table，添加合并表
  // 依次遍历每个 route
  //
  // "replica-1.route_rules.1": {
  //   "schema-pattern": "_gravity",
  //   "table-pattern": "",
  //   "target-schema": "_gravity222",
  //   "target-table": ""
  // },
  // "replica-1.route_rules.2": {
  //   "schema-pattern": "_gravity",
  //   "table-pattern": "gravity_heartbeat_v2",
  //   "target-schema": "_gravity222",
  //   "target-table": "newtable"
  // }
  //
  // 对 schema 的重建
  // 先根据 target-schema 字段的值 (假设为上例中的 _gravity222)，创建出一个初始版本的 schema
  // 因为我们还不知道它到底是一个新生成的 schema，还是从上游的 source schema 生成的
  // 所以先假定它是新生成的，{ sourceId: 'new', schema: 'newdatabase', newName: xxx, ... }
  // key 的值无意义，只要保证唯一就行，target-schema 的值在下游是唯一不能重复的，所以可以设定 key 为 unknown:_gravity222
  // 直到如果遇到了 table-pattern 和 target-table 都为空的同名 schema 时，此时可以判断 schema 是由上游 source schema 生成的
  // 修改此 schema 的 sourceId/schema
  //
  // 对 table 的重建
  // 我们同样面临一个 table 是属于重命名的情况，还是和其它表被合并到一个合并表中的情况
  // 我们分两轮来处理
  // 先假设都是重命名的情况，即 schema 下都是原始表，没有合并表
  // 处理完后，再对比 schema 下的所有表，找出同名的表，把这些同名的表进行合并
  const targetSchemas: IFullSchemas = {}
  const routes: IRoutes = finalConfig.routes
  const mergedTables: {
    [mergedTableKey: string]: string[]
  } = {}
  Object.keys(routes).forEach(routeKey => {
    // routeKey => "replica-1.route_rules.1"
    const sourceId: string = routeKey.split('.')[0]
    const route: IRoute = routes[routeKey]
    const oriSchemaName = route['schema-pattern']
    const newSchemaName = route['target-schema'] // newSchemaName 在下游不会重复
    const oriTableName = route['table-pattern']
    const newTableName = route['target-table']

    const schemaKey = `new:${newSchemaName}`
    const schema =
      targetSchemas[schemaKey] || genDefFullSchema('new', newSchemaName, true)
    targetSchemas[schemaKey] = schema

    if (oriTableName === '' && newTableName === '') {
      // 说明是由上游 schema 生成的
      schema.sourceId = sourceId
      schema.schema = oriSchemaName
      return
    }

    // table
    const oriTableKey = `${sourceId}:${oriSchemaName}:${oriTableName}`
    const table = allTables[oriTableKey]
    table.newName = newTableName!
    table.parentKey = schemaKey
    schema.tables.push(oriTableKey)

    // 假设这个 route 有可能是合并表
    const mergedTableKey = `new:${newSchemaName}:${newTableName}`
    mergedTables[mergedTableKey] = (mergedTables[mergedTableKey] || []).concat(
      oriTableKey
    )
  })
  // 对 mergedTables 进行遍历，如果某个 key 对应的数组的长度超过 1，则为合并表
  // 此时要做三件事
  // 1. 相应的 schema 要从 tables 数组移除子表 key，加入新的合并表 key
  // 2. 生成一张合并表，将 parentKey 指向相应的 schemaKey，mergedTables 为对应的数组，将表加入 allTables
  // 3. 将对应数组中的 table key 指向的 table 的 parentKey 指向新建的合并表，并将 newName 重置为原始的名字
  console.log(mergedTables)
  Object.keys(mergedTables).forEach(mergedTableKey => {
    const childTableKeys: string[] = mergedTables[mergedTableKey]
    if (childTableKeys.length === 1) {
      return
    }
    // else > 1，是合并表
    // 1. 修改 schema tables
    const nameArr: string[] = mergedTableKey.split(':')
    const schemaKey = nameArr.slice(0, 2).join(':')
    const schema = targetSchemas[schemaKey]
    schema.tables = schema.tables
      .filter(t => !childTableKeys.includes(t))
      .concat(mergedTableKey)
    // 2. 生成合并表
    const newTable: IFullTable = {
      type: 'mergedTable',
      key: mergedTableKey,
      sourceId: 'new',
      schema: 'newdatabase',
      table: 'newtable',
      newName: nameArr[2],
      parentKey: schemaKey,
      mergedTables: childTableKeys,
      filters: []
    }
    allTables[mergedTableKey] = newTable
    // 3. 修改子表的 parentKey
    childTableKeys.forEach(childTableKey => {
      const childTable = allTables[childTableKey]
      childTable.parentKey = mergedTableKey
      childTable.newName = childTable.table
    })
  })

  // 第三步：从 filters 中还原 binlog 过滤规则 (这个简单些)
  const filterRules: IFilters = finalConfig.filters
  Object.keys(filterRules).forEach(filterKey => {
    const sourceId = filterKey.split('.')[0]
    const filterRule = filterRules[filterKey]
    if (filterRule['table-pattern'] === '') {
      // 说明此条 filter 是属于 schema 的
      const schemaKey = `${sourceId}:${filterRule['schema-pattern']}`
      const schema = sourceSchemas[schemaKey]
      schema.filters = filterRule.events
    } else {
      // 说明此条 filter 属于 table
      const tableKey = `${sourceId}:${filterRule['schema-pattern']}:${filterRule['table-pattern']}`
      const table = allTables[tableKey]
      table.filters = filterRule.events
    }
  })

  return {
    taskInfo,
    instances,
    sourceFullInstances,
    sourceSchemas,
    allTables,
    targetSchemas
  }
}

/////////////////////////

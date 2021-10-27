import request from '../utils/request'
import { IDatabase, ISourceInstance } from '../types'

const BASE_API_URL = ''

function fullUrl(path: string) {
  return `${BASE_API_URL}/${path}`
}

export function checkTargetInstance(dbConfig: IDatabase) {
  return request(fullUrl('check'), 'POST', dbConfig)
}

export function checkSourceInstance(instance: ISourceInstance) {
  return request(fullUrl('schema'), 'POST', instance.dbConfig)
}

export function generateConfig(finalConfig: any) {
  return request(fullUrl('generate_config'), 'POST', finalConfig)
}

export function downloadConfig(serverFilePath: string) {
  const downloadLink = document.createElement('a')
  let url = `/download?filepath=${serverFilePath}`
  // 下载请求不是 ajax 请求，不会走代理转发，所以在开发环境时要显式地声明 host:port
  if (process.env.NODE_ENV === 'development') {
    url = 'http://localhost:4001' + url
  }
  const localFileName = serverFilePath.split('/').pop()
  downloadLink.href = url
  downloadLink.download = localFileName as string
  downloadLink.click()
}

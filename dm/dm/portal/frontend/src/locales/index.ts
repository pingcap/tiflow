import zhCN from './zh-CN'
import enUS from './en-US'

export default (locale: string) => {
  if (locale.startsWith('zh')) return zhCN
  return enUS
}

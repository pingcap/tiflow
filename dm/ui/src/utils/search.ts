import { useEffect, useRef, useState } from 'react'
import Fuse from 'fuse.js'
import { useDebounce } from 'ahooks'

export function useFuseSearch<T>(list?: T[], options?: Fuse.IFuseOptions<T>) {
  const [keyword, setKeyword] = useState('')
  const fuseRef = useRef<Fuse<T>>()
  const [result, setResult] = useState<T[]>([])
  const debouncedKeyword = useDebounce(keyword, { wait: 600 })
  const searchResult = () => {
    const val = debouncedKeyword?.trim()
    if (val && fuseRef.current) {
      setResult(fuseRef.current.search(val).map(i => i.item))
    } else {
      setResult(list ?? [])
    }
  }

  useEffect(() => {
    if (list && list.length > 0) {
      fuseRef.current = new Fuse(list, options)
    }
    searchResult()
  }, [list])

  useEffect(() => {
    searchResult()
  }, [debouncedKeyword])

  return {
    result,
    keyword,
    setKeyword,
  }
}

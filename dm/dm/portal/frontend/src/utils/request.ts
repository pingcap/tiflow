type IOriRes = {
  result: 'success' | 'failed'
  error: string
  [key: string]: any
}

type IRes = {
  data?: any
  err?: Error
}

function parseResponse(response: Response) {
  if (response.status === 204) {
    return {}
  } else if (response.status >= 200 && response.status < 300) {
    return response.json().then((resData: IOriRes) => {
      if (resData.result === 'success') {
        return resData
      } else if (resData.result === 'failed') {
        throw new Error(resData.error)
      }
    })
  } else {
    return response.json().then((resData: IOriRes) => {
      const errMsg = resData.error || response.statusText
      throw new Error(errMsg)
    })
  }
}

function doFetch(url: string, options: RequestInit): Promise<IRes> {
  return fetch(url, options)
    .then(parseResponse)
    .then(data => ({ data }))
    .catch(err => ({ err }))
}

export default function request(
  url: string,
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE',
  body?: object,
  options?: RequestInit
): Promise<IRes> {
  const opts: RequestInit = {
    ...options,
    method: method || 'GET',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json'
    }
  }
  if (body) {
    opts.body = JSON.stringify(body)
  }
  return doFetch(url, opts)
}

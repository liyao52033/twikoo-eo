/*!
 * Twikoo EdgeOne Pages Logger
 * 专为 EdgeOne Pages Node Function 设计的日志模块
 */

const LOG_PREFIX = '[Twikoo]'

/**
 * 格式化日志参数，移除 undefined 值
 */
function formatParams(params) {
  if (!params || typeof params !== 'object') return params
  const result = {}
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      result[key] = value
    }
  }
  return result
}

/**
 * 记录请求日志
 */
function logRequest(method, path, search) {
  console.log(LOG_PREFIX, 'Request:', { method, path, search: search || '' })
}

/**
 * 记录 POST 事件日志
 */
function logEvent(event, ip, params = {}) {
  const logData = formatParams({
    event: event || 'UNKNOWN',
    ip,
    ...params
  })
  console.log(LOG_PREFIX, 'Event:', logData)
}

/**
 * 记录响应日志
 */
function logResponse(event, code, extra = {}) {
  const logData = formatParams({
    event,
    code: code || 0,
    ...extra
  })
  console.log(LOG_PREFIX, 'Response:', logData)
}

/**
 * 记录错误日志
 */
function logError(message, error) {
  console.error(LOG_PREFIX, 'Error:', message, error?.message || error)
}

/**
 * 记录警告日志
 */
function logWarn(message, detail) {
  console.warn(LOG_PREFIX, 'Warn:', message, detail || '')
}

/**
 * 记录信息日志
 */
function logInfo(message, detail) {
  console.log(LOG_PREFIX, 'Info:', message, detail || '')
}

/**
 * 兼容 twikoo-func 的 logger 对象
 */
const logger = {
  log: (message, ...args) => console.log(LOG_PREFIX, message, ...args),
  info: (message, ...args) => console.log(LOG_PREFIX, 'Info:', message, ...args),
  warn: (message, ...args) => console.warn(LOG_PREFIX, 'Warn:', message, ...args),
  error: (message, ...args) => console.error(LOG_PREFIX, 'Error:', message, ...args)
}

export {
  logRequest,
  logEvent,
  logResponse,
  logError,
  logWarn,
  logInfo,
  logger
}

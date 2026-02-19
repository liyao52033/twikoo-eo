/*!
 * Twikoo EdgeOne Pages Node Function
 * (c) 2020-present iMaeGoo
 * Released under the MIT License.
 * 
 * 使用 twikoo-func 实现核心逻辑，通过 Edge Function 操作 supabase 数据库
 */

import { v4 as uuidv4 } from 'uuid'
import xss from 'xss'
import bowser from 'bowser'
import { Resend } from 'resend'
import { createClient } from '@supabase/supabase-js'
import {
  getMd5,
  getSha256,
  getXml2js,
  setCustomLibs
} from 'twikoo-func/utils/lib'
import { getIpRegion } from './ip2region-searcher.js'
import { logRequest, logEvent, logResponse, logError, logger } from './logger.js'
import {
  getFuncVersion,
  getUrlQuery,
  getUrlsQuery,
  normalizeMail,
  equalsMail,
  getMailMd5,
  getAvatar,
  isQQ,
  addQQMailSuffix,
  getQQAvatar,
  getPasswordStatus,
  checkTurnstileCaptcha,
  getConfig,
  getConfigForAdmin,
  validate
} from 'twikoo-func/utils'
import {
  jsonParse,
  commentImportValine,
  commentImportDisqus,
  commentImportArtalk,
  commentImportArtalk2,
  commentImportTwikoo
} from 'twikoo-func/utils/import'
import { postCheckSpam as originalPostCheckSpam } from 'twikoo-func/utils/spam'
import { sendNotice, emailTest } from 'twikoo-func/utils/notify'

// 包装 postCheckSpam 函数，处理 MANUAL_REVIEW 模式
async function postCheckSpam(comment, config) {
  // 如果是人工审核模式，直接返回预检测的结果
  if (config.AKISMET_KEY === 'MANUAL_REVIEW') {
    logger.info('人工审核模式，跳过 postCheckSpam，使用预检测结果:', comment.is_spam)
    return comment.is_spam
  }
  return originalPostCheckSpam(comment, config)
}

import constants from 'twikoo-func/utils/constants'

// ==================== 兼容 EdgeOne 环境的图片上传功能 ====================

/**
 * 将 base64 数据转换为 Blob 对象
 */
function base64ToBlob(base64Url, fileName) {
  const base64 = base64Url.split(';base64,').pop()
  const byteCharacters = atob(base64)
  const byteNumbers = new Array(byteCharacters.length)
  for (let i = 0; i < byteCharacters.length; i++) {
    byteNumbers[i] = byteCharacters.charCodeAt(i)
  }
  const byteArray = new Uint8Array(byteNumbers)
  return new Blob([byteArray])
}

/**
 * 检查是否为有效的 URL
 */
function isUrl(string) {
  try {
    new URL(string)
    return true
  } catch (_) {
    return false
  }
}

/**
 * 上传图片到 SM.MS 图床
 */
async function uploadImageToSmms(photo, fileName, config, imageCdn) {
  const blob = base64ToBlob(photo, fileName)
  const formData = new FormData()
  formData.append('smfile', blob, fileName)

  const response = await fetch(imageCdn, {
    method: 'POST',
    headers: {
      'Authorization': config.IMAGE_CDN_TOKEN
    },
    body: formData
  })

  const result = await response.json()
  if (result.success) {
    return { data: result.data }
  } else {
    throw new Error(result.message || '上传失败')
  }
}

/**
 * 上传图片到兰空图床(Lsky Pro)
 */
async function uploadImageToLskyPro(photo, fileName, config, imageCdn) {
  const blob = base64ToBlob(photo, fileName)
  const formData = new FormData()
  formData.append('file', blob, fileName)

  if (process.env.TWIKOO_LSKY_STRATEGY_ID) {
    formData.append('strategy_id', parseInt(process.env.TWIKOO_LSKY_STRATEGY_ID))
  }

  const url = `${imageCdn}/api/v1/upload`
  let token = config.IMAGE_CDN_TOKEN
  if (!token.startsWith('Bearer')) {
    token = `Bearer ${token}`
  }

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': token
    },
    body: formData
  })

  const result = await response.json()
  if (result.status) {
    return {
      data: {
        ...result.data,
        url: result.data.links.url
      }
    }
  } else {
    throw new Error(result.message || '上传失败')
  }
}

/**
 * 上传图片到 PicList
 * PicList 是一款图床管理软件，需要开启上传服务后使用
 * 上传服务默认地址: http://localhost:36677
 * 配置说明: https://piclist.cn/configure.html
 */
async function uploadImageToPicList(photo, fileName, config, imageCdn) {
  const blob = base64ToBlob(photo, fileName)
  const formData = new FormData()
  formData.append('file', blob, fileName)

  let url = `${imageCdn}/upload`
  if (config.IMAGE_CDN_TOKEN) {
    url += `?key=${config.IMAGE_CDN_TOKEN}`
  }

  const response = await fetch(url, {
    method: 'POST',
    body: formData
  })

  const result = await response.json()
  if (result.success) {
    return {
      data: {
        ...result,
        url: result.result[0]
      }
    }
  } else {
    throw new Error(result.message || '上传失败')
  }
}

/**
 * 上传图片到 PicGo
 * PicGo 是一款图床管理软件，需要开启「PicGo-Server」后使用
 * 上传服务默认地址: http://localhost:36677
 * 配置说明: https://picgo.github.io/PicGo-Doc/zh/guide/config.html#picgo-server
 */
async function uploadImageToPicGo(photo, fileName, config, imageCdn) {
  const blob = base64ToBlob(photo, fileName)
  const formData = new FormData()
  formData.append('file', blob, fileName)

  // PicGo API 不需要 key 参数，而是使用 list 参数来指定图床
  const url = `${imageCdn}/upload`

  const response = await fetch(url, {
    method: 'POST',
    body: formData
  })

  const result = await response.json()
  // PicGo 返回格式: { success: true, result: ['url1', 'url2'] }
  if (result.success && result.result && result.result.length > 0) {
    return {
      data: {
        url: result.result[0],
        fullResult: result
      }
    }
  } else {
    throw new Error(result.message || '上传失败')
  }
}

/**
 * 上传图片到 GitHub
 * 配置要求:
 * - IMAGE_CDN_URL: GitHub 仓库路径，格式: owner/repo/branch/path (如: username/images/main/img)
 * - IMAGE_CDN_TOKEN: GitHub Personal Access Token (需要 repo 权限)
 */
async function uploadImageToGitHub(photo, fileName, config) {
  if (!config.IMAGE_CDN_URL) {
    throw new Error('未配置 GitHub 仓库路径 (IMAGE_CDN_URL)，格式: owner/repo/branch/path')
  }
  if (!config.IMAGE_CDN_TOKEN) {
    throw new Error('未配置 GitHub Token (IMAGE_CDN_TOKEN)')
  }

  // 解析仓库路径: owner/repo/branch/path
  const parts = config.IMAGE_CDN_URL.split('/')
  if (parts.length < 3) {
    throw new Error('GitHub 仓库路径格式错误，应为: owner/repo/branch/path')
  }

  const owner = parts[0]
  const repo = parts[1]
  const branch = parts[2]
  const path = parts.slice(3).join('/') || ''

  // 生成唯一文件名
  const timestamp = Date.now()
  const uniqueFileName = `${timestamp}_${fileName}`
  const fullPath = path ? `${path}/${uniqueFileName}` : uniqueFileName

  // 提取 base64 内容
  const base64Content = photo.split(';base64,').pop()

  // 调用 GitHub API 上传文件
  const url = `https://api.github.com/repos/${owner}/${repo}/contents/${fullPath}`

  const response = await fetch(url, {
    method: 'PUT',
    headers: {
      'Authorization': `token ${config.IMAGE_CDN_TOKEN}`,
      'Content-Type': 'application/json',
      'User-Agent': 'Twikoo'
    },
    body: JSON.stringify({
      message: `Upload image via Twikoo: ${fileName}`,
      content: base64Content,
      branch: branch
    })
  })

  const result = await response.json()

  if (!response.ok) {
    if (result.message && result.message.includes('already exists')) {
      throw new Error('文件已存在，请重试')
    }
    throw new Error(`GitHub API 错误: ${result.message || response.statusText}`)
  }

  // 返回图片 URL (使用 jsDelivr CDN 加速)
  const rawUrl = result.content.download_url
  const cdnUrl = rawUrl.replace(
    'https://raw.githubusercontent.com/',
    'https://cdn.jsdelivr.net/gh/'
  ).replace(`/${branch}/`, `@${branch}/`)

  return {
    data: {
      url: cdnUrl,
      raw_url: rawUrl,
      html_url: result.content.html_url,
      sha: result.content.sha
    }
  }
}

/**
 * AWS Signature V4 签名辅助函数
 */
function getSignatureKey(key, dateStamp, regionName, serviceName) {
  const kDate = hmacSHA256(`AWS4${key}`, dateStamp)
  const kRegion = hmacSHA256(kDate, regionName)
  const kService = hmacSHA256(kRegion, serviceName)
  const kSigning = hmacSHA256(kService, 'aws4_request')
  return kSigning
}

function hmacSHA256(key, data) {
  const crypto = require('crypto')
  return crypto.createHmac('sha256', key).update(data).digest()
}

function sha256Hash(data) {
  const crypto = require('crypto')
  return crypto.createHash('sha256').update(data).digest('hex')
}

function toHex(buffer) {
  return buffer.toString('hex')
}

/**
 * 上传图片到 S3 兼容存储 (hi168, AWS S3, 阿里云 OSS, 腾讯云 COS 等)
 * 配置要求:
 * - IMAGE_CDN_URL: 完整的 S3 URL，格式: https://endpoint/bucket/region/path
 *   示例: https://s3.hi168.com/hi168-25202-9063qibb/us-east-1/picgo
 * - IMAGE_CDN_TOKEN: 格式: accessKeyId:secretAccessKey
 */
async function uploadImageToS3(photo, fileName, config) {
  if (!config.IMAGE_CDN_URL) {
    throw new Error('未配置 S3 URL (IMAGE_CDN_URL)，格式: https://endpoint/bucket/region/path')
  }
  if (!config.IMAGE_CDN_TOKEN) {
    throw new Error('未配置 S3 密钥 (IMAGE_CDN_TOKEN)，格式: accessKeyId:secretAccessKey')
  }

  // 解析密钥
  const tokenParts = config.IMAGE_CDN_TOKEN.split(':')
  if (tokenParts.length !== 2) {
    throw new Error('S3 密钥格式错误，应为: accessKeyId:secretAccessKey')
  }
  const accessKeyId = tokenParts[0]
  const secretAccessKey = tokenParts[1]

  // 解析 URL: https://endpoint/bucket/region/path
  const urlObj = new URL(config.IMAGE_CDN_URL)
  const endpoint = `${urlObj.protocol}//${urlObj.host}`
  const pathParts = urlObj.pathname.split('/').filter(p => p)

  if (pathParts.length < 2) {
    throw new Error('S3 URL 格式错误，应为: https://endpoint/bucket/region/path')
  }

  const bucket = pathParts[0]
  const region = pathParts[1]
  const pathPrefix = pathParts.slice(2).join('/')

  // 生成唯一文件名
  const timestamp = Date.now()
  const uniqueFileName = `${timestamp}_${fileName}`
  const objectKey = pathPrefix ? `${pathPrefix}/${uniqueFileName}` : uniqueFileName

  // 提取二进制数据
  const base64 = photo.split(';base64,').pop()
  const binaryString = atob(base64)
  const bytes = new Uint8Array(binaryString.length)
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i)
  }

  // 获取内容类型
  const contentType = photo.match(/data:(.*?);base64/)?.[1] || 'image/jpeg'

  // AWS Signature V4 签名
  const now = new Date()
  const dateStamp = now.toISOString().slice(0, 10).replace(/-/g, '')
  const timeStamp = now.toISOString().replace(/[:-]/g, '').slice(0, 15) + 'Z'
  const host = urlObj.host

  // 计算 payload hash
  const payloadHash = sha256Hash(Buffer.from(bytes))

  // 构建规范请求
  const canonicalUri = `/${bucket}/${objectKey}`
  const canonicalQuerystring = ''
  const canonicalHeaders = `host:${host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${timeStamp}\n`
  const signedHeaders = 'host;x-amz-content-sha256;x-amz-date'
  const canonicalRequest = `PUT\n${canonicalUri}\n${canonicalQuerystring}\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`

  // 构建待签名字符串
  const algorithm = 'AWS4-HMAC-SHA256'
  const credentialScope = `${dateStamp}/${region}/s3/aws4_request`
  const stringToSign = `${algorithm}\n${timeStamp}\n${credentialScope}\n${sha256Hash(canonicalRequest)}`

  // 计算签名
  const signingKey = getSignatureKey(secretAccessKey, dateStamp, region, 's3')
  const signature = toHex(hmacSHA256(signingKey, stringToSign))

  // 构建 Authorization header
  const authorizationHeader = `${algorithm} Credential=${accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`

  // 发送 PUT 请求
  const url = `${endpoint}/${bucket}/${objectKey}`

  const response = await fetch(url, {
    method: 'PUT',
    headers: {
      'Host': host,
      'Content-Type': contentType,
      'x-amz-date': timeStamp,
      'x-amz-content-sha256': payloadHash,
      'Authorization': authorizationHeader
    },
    body: bytes
  })

  if (!response.ok) {
    const errorText = await response.text()
    throw new Error(`S3 上传失败: ${response.status} ${response.statusText} - ${errorText}`)
  }

  // 构建访问 URL
  const fileUrl = `${endpoint}/${bucket}/${objectKey}`

  return {
    data: {
      url: fileUrl,
      key: objectKey,
      bucket: bucket
    }
  }
}

/**
 * 上传图片到 EasyImage 2.0
 */
async function uploadImageToEasyImage(photo, fileName, config) {
  if (!config.IMAGE_CDN_URL) {
    throw new Error('未配置 EasyImage2.0 的 API 地址 (IMAGE_CDN_URL)')
  }
  if (!config.IMAGE_CDN_TOKEN) {
    throw new Error('未配置 EasyImage2.0 的 Token (IMAGE_CDN_TOKEN)')
  }

  const blob = base64ToBlob(photo, fileName)
  const formData = new FormData()
  formData.append('token', config.IMAGE_CDN_TOKEN)
  formData.append('image', blob, fileName)

  const response = await fetch(config.IMAGE_CDN_URL, {
    method: 'POST',
    headers: {
      'User-Agent': 'Twikoo'
    },
    body: formData
  })

  const result = await response.json()
  if (result.code === 200 && result.result === 'success') {
    return {
      data: {
        url: result.url,
        thumb: result.thumb,
        del: result.del
      }
    }
  } else {
    throw new Error(`API 返回错误 (CODE: ${result.code})`)
  }
}

/**
 * 主上传函数 - 兼容 EdgeOne 环境
 */
async function uploadImage(event, config) {
  const { photo, fileName } = event
  const res = {}

  try {
    if (!config.IMAGE_CDN) {
      throw new Error('未配置图片上传服务 (IMAGE_CDN)')
    }

    // tip: qcloud 图床也支持后端上传
    if (config.IMAGE_CDN === 'qcloud') {
      // 腾讯云 COS - 使用 S3 兼容接口
      if (!config.IMAGE_CDN_URL) {
        throw new Error('未配置腾讯云 COS 信息 (IMAGE_CDN_URL)，格式: https://cos.region.myqcloud.com/bucket/region/path')
      }
      const result = await uploadImageToS3(photo, fileName, config)
      res.data = result.data
    } else if (config.IMAGE_CDN === '7bu') {
      const result = await uploadImageToLskyPro(photo, fileName, config, 'https://7bu.top')
      res.data = result.data
    } else if (config.IMAGE_CDN === 'smms') {
      const result = await uploadImageToSmms(photo, fileName, config, 'https://smms.app/api/v2/upload')
      res.data = result.data
    } else if (isUrl(config.IMAGE_CDN)) {
      const result = await uploadImageToLskyPro(photo, fileName, config, config.IMAGE_CDN)
      res.data = result.data
    } else if (config.IMAGE_CDN === 'lskypro') {
      if (!config.IMAGE_CDN_URL) {
        throw new Error('未配置兰空图床 URL (IMAGE_CDN_URL)')
      }
      const result = await uploadImageToLskyPro(photo, fileName, config, config.IMAGE_CDN_URL)
      res.data = result.data
    } else if (config.IMAGE_CDN === 'piclist') {
      if (!config.IMAGE_CDN_URL) {
        throw new Error('未配置 PicList URL (IMAGE_CDN_URL)')
      }
      const result = await uploadImageToPicList(photo, fileName, config, config.IMAGE_CDN_URL)
      res.data = result.data
    } else if (config.IMAGE_CDN === 'picgo') {
      if (!config.IMAGE_CDN_URL) {
        throw new Error('未配置 PicGo URL (IMAGE_CDN_URL)')
      }
      const result = await uploadImageToPicGo(photo, fileName, config, config.IMAGE_CDN_URL)
      res.data = result.data
    } else if (config.IMAGE_CDN === 'github') {
      const result = await uploadImageToGitHub(photo, fileName, config)
      res.data = result.data
    } else if (config.IMAGE_CDN === 's3') {
      const result = await uploadImageToS3(photo, fileName, config)
      res.data = result.data
    } else if (config.IMAGE_CDN === 'easyimage') {
      const result = await uploadImageToEasyImage(photo, fileName, config)
      res.data = result.data
    } else {
      throw new Error(`不支持的图片上传服务: ${config.IMAGE_CDN}`)
    }
  } catch (e) {
    logger.error('图片上传失败:', e)
    res.code = RES_CODE.UPLOAD_FAILED
    res.err = e.message
  }

  return res
}

const { RES_CODE } = constants
const VERSION = '1.6.44'

// 自定义 preCheckSpam 函数，确保人工审核模式正确工作
function preCheckSpamWithLog(event, config) {
  logger.info('调用 preCheckSpam，传入配置:', {
    AKISMET_KEY: config.AKISMET_KEY,
    LIMIT_LENGTH: config.LIMIT_LENGTH,
    FORBIDDEN_WORDS: config.FORBIDDEN_WORDS,
    BLOCKED_WORDS: config.BLOCKED_WORDS
  })

  try {
    const { comment, nick } = { comment: event.comment, nick: event.nick }

    // 长度限制
    let limitLength = parseInt(config.LIMIT_LENGTH)
    if (Number.isNaN(limitLength)) limitLength = 500
    if (limitLength && comment.length > limitLength) {
      throw new Error('评论内容过长')
    }

    // 屏蔽词检测
    if (config.BLOCKED_WORDS) {
      const commentLowerCase = comment.toLowerCase()
      const nickLowerCase = nick.toLowerCase()
      for (const blockedWord of config.BLOCKED_WORDS.split(',')) {
        const blockedWordLowerCase = blockedWord.trim().toLowerCase()
        if (blockedWordLowerCase && (commentLowerCase.indexOf(blockedWordLowerCase) !== -1 || nickLowerCase.indexOf(blockedWordLowerCase) !== -1)) {
          throw new Error('包含屏蔽词')
        }
      }
    }

    // 人工审核模式 - 所有评论都标记为需要审核
    logger.info('检查人工审核模式，AKISMET_KEY:', config.AKISMET_KEY, '是否等于 MANUAL_REVIEW:', config.AKISMET_KEY === 'MANUAL_REVIEW')
    if (config.AKISMET_KEY === 'MANUAL_REVIEW') {
      logger.info('已使用人工审核模式，评论审核后才会发表~')
      return true
    }

    // 违禁词检测
    if (config.FORBIDDEN_WORDS) {
      const commentLowerCase = comment.toLowerCase()
      const nickLowerCase = nick.toLowerCase()
      for (const forbiddenWord of config.FORBIDDEN_WORDS.replace(/,+$/, '').split(',')) {
        const forbiddenWordLowerCase = forbiddenWord.trim().toLowerCase()
        if (forbiddenWordLowerCase && (commentLowerCase.indexOf(forbiddenWordLowerCase) !== -1 || nickLowerCase.indexOf(forbiddenWordLowerCase) !== -1)) {
          logger.warn('包含违禁词，直接标记为垃圾评论~')
          return true
        }
      }
    }

    logger.info('preCheckSpam 执行结果: false，AKISMET_KEY:', config.AKISMET_KEY)
    return false
  } catch (error) {
    logger.error('preCheckSpam 执行出错:', error.message)
    throw error
  }
}

// Supabase 配置
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY

// 创建 Supabase 客户端
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

// 注入自定义依赖（对标 Cloudflare 版本）
setCustomLibs({
  DOMPurify: {
    sanitize(input) {
      return input
    }
  },
  nodemailer: {
    createTransport(mailConfig) {
      return {
        verify() {
          const supportedServices = [
            '126', '163', '1und1', 'AOL', 'DebugMail', 'DynectEmail', 'FastMail', 'GandiMail',
            'Gmail', 'Godaddy', 'GodaddyAsia', 'GodaddyEurope', 'Hotmail', 'Mail.ru', 'Maildev',
            'Mailgun', 'Mailjet', 'Mailosaur', 'Mandrill', 'Naver', 'OpenMailBox', 'Outlook365',
            'Postmark', 'QQ', 'QQex', 'SES', 'SES-EU-WEST-1', 'SES-US-EAST-1', 'SES-US-WEST-2',
            'SendCloud', 'SendGrid', 'SendPulse', 'SendinBlue', 'Sparkpost', 'Yahoo', 'Yandex',
            'Zoho', 'hot.ee', 'iCloud', 'mail.ee', 'qiye.aliyun', 'mailchannels'
          ]
          if (!mailConfig.service || !supportedServices.includes(mailConfig.service)) {
            throw new Error(`仅支持官方列出的邮件服务。`)
          }
          if (!mailConfig.auth || !mailConfig.auth.user) {
            throw new Error('需要在 SMTP_USER 中配置账户名，如果邮件服务不需要可随意填写。')
          }
          if (!mailConfig.auth || !mailConfig.auth.pass) {
            throw new Error('需要在 SMTP_PASS 中配置 API 令牌或密码。')
          }
          return true
        },
        async sendMail({ from, to, subject, html }) {
          const service = mailConfig.service.toLowerCase()
          let response

          try {
            if (service === 'sendgrid') {
              // 使用 SendGrid API
              response = await fetch('https://api.sendgrid.com/v3/mail/send', {
                method: 'POST',
                headers: {
                  'Authorization': `Bearer ${mailConfig.auth.pass}`,
                  'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                  personalizations: [{ to: [{ email: to }] }],
                  from: { email: from },
                  subject,
                  content: [{ type: 'text/html', value: html }],
                })
              })
            } else if (service === 'mailchannels') {
              // 使用 MailChannels API
              response = await fetch('https://api.mailchannels.net/tx/v1/send', {
                method: 'POST',
                headers: {
                  'X-Api-Key': mailConfig.auth.pass,
                  'Accept': 'application/json',
                  'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                  personalizations: [{ to: [{ email: to }] }],
                  from: { email: from },
                  subject,
                  content: [{ type: 'text/html', value: html }],
                })
              })
            } else {
              // 对于其他所有邮件服务，使用 resend API 作为后端
              // 注意：这仍然需要在 https://resend.com/  注册并配置 API 密钥
              const resend = new Resend(mailConfig.auth.pass);
              const { data, error } = await resend.emails.send({
                from: mailConfig.auth.user,
                to: [config.BLOGGER_EMAIL],
                subject,
                html
              })

              if (error) {
                throw new Error(`resend邮件发送失败: ${error.message}`);
              }

              return data
            }

            if (!response) {
              throw new Error('邮件发送服务未配置')
            }

            if (!response.ok) {
              const errorData = await response.json().catch(() => ({}))
              throw new Error(`邮件发送失败: ${response.status} ${response.statusText} ${errorData.message || ''}`)
            }

            return response
          } catch (error) {
            logger.error('邮件发送失败:', error.message)
            throw new Error(`邮件发送失败: ${error.message}`)
          }
        }
      }
    }
  }
})

const md5 = getMd5()
const sha256 = getSha256()
const xml2js = getXml2js()

// ==================== 本地实现的 parseComment（替代 twikoo-func 版本）====================

/**
 * 修复 OS 版本名称
 */
function fixOS(ua) {
  const os = ua.getOS()
  if (!os.versionName) {
    if (os.name === 'Windows' && os.version === 'NT 11.0') {
      os.versionName = '11'
    } else if (os.name === 'macOS') {
      const majorPlatformVersion = os.version?.split('.')[0]
      os.versionName = {
        11: 'Big Sur', 12: 'Monterey', 13: 'Ventura', 14: 'Sonoma', 15: 'Sequoia'
      }[majorPlatformVersion]
    } else if (os.name === 'Android') {
      const majorPlatformVersion = os.version?.split('.')[0]
      os.versionName = {
        10: 'Quince Tart', 11: 'Red Velvet Cake', 12: 'Snow Cone',
        13: 'Tiramisu', 14: 'Upside Down Cake', 15: 'Vanilla Ice Cream', 16: 'Baklava'
      }[majorPlatformVersion]
    } else if (ua.test(/harmony/i)) {
      os.name = 'Harmony'
      const match = ua.getUA().match(/harmony[\s/-](\d+(\.\d+)*)/i)
      os.version = (match && match[1]) || ''
      os.versionName = ''
    }
  }
  return os
}

/**
 * 获取回复人昵称
 */
function getRuser(pid, comments = []) {
  const comment = comments.find((item) => item.id === pid)
  return comment ? comment.nick : null
}

/**
 * 将评论记录转换为前端需要的格式（使用本地 IP 归属地查询）
 */
function toCommentDto(comment, uid, replies = [], comments = [], cfg) {
  let displayOs = ''
  let displayBrowser = ''
  if (cfg.SHOW_UA !== 'false') {
    try {
      const ua = bowser.getParser(comment.ua)
      const os = fixOS(ua)
      displayOs = [os.name, os.versionName ? os.versionName : os.version].join(' ')
      displayBrowser = [ua.getBrowserName(), ua.getBrowserVersion()].join(' ')
    } catch (e) {
      logger.warn('bowser 错误：', e)
    }
  }
  const showRegion = !!cfg.SHOW_REGION && cfg.SHOW_REGION !== 'false'
  return {
    id: comment.id.toString(),
    nick: comment.nick,
    avatar: comment.avatar,
    mailMd5: getMailMd5(comment),
    link: comment.link,
    comment: comment.comment,
    os: displayOs,
    browser: displayBrowser,
    ipRegion: showRegion ? getIpRegion(comment.ip, false) : '',
    master: comment.master,
    like: comment.likes ? comment.likes.length : 0,
    liked: comment.likes ? comment.likes.findIndex((item) => item === uid) > -1 : false,
    replies: replies,
    rid: comment.rid,
    pid: comment.pid,
    ruser: getRuser(comment.pid, comments),
    top: comment.top,
    isSpam: !!(comment.isSpam !== undefined ? comment.isSpam : comment.is_spam), // 确保是布尔值
    created: comment.created,
    updated: comment.updated
  }
}

/**
 * 筛除隐私字段，拼接回复列表（本地实现，使用自己的 IP 归属地查询）
 * @param {Array} comments - 评论列表
 * @param {string} uid - 用户ID
 * @param {Object} cfg - 配置
 * @param {boolean} isAdmin - 是否是管理员
 */
function parseComment(comments, uid, cfg, isAdmin = false) {
  const result = []
  for (const comment of comments) {
    if (!comment.rid) {
      // 检查主楼评论是否是审核中状态
      const isMainSpam = !!(comment.isSpam !== undefined ? comment.isSpam : comment.is_spam)
      // 如果是审核中的主楼评论，只有发布者本人以及管理员能看到
      if (isMainSpam && !isAdmin) {
        // 使用 uid 验证
        if (!uid || comment.uid !== uid) continue
      }

      // 过滤回复：审核中的评论只有发布者本人以及管理员能看到
      const replies = comments
        .filter((item) => item.rid === comment.id.toString())
        .filter((item) => {
          // 如果不是审核中的评论，直接显示
          const isSpam = !!(item.isSpam !== undefined ? item.isSpam : item.is_spam)
          if (!isSpam) return true
          // 管理员可以看到所有审核中的回复
          if (isAdmin) return true
          // 如果是审核中的评论，使用 uid 验证
          return uid && item.uid === uid
        })
        .map((item) => toCommentDto(item, uid, [], comments, cfg))
        .sort((a, b) => a.created - b.created)
      result.push(toCommentDto(comment, uid, replies, [], cfg))
    }
  }
  return result
}

/**
 * 为管理后台解析评论
 */
function parseCommentForAdmin(comments) {
  for (const comment of comments) {
    comment.ipRegion = getIpRegion(comment.ip, true)
    comment.isParent = !comment.rid
    if (!comment._id && comment.id) {
      comment._id = comment.id
    }
  }
  return comments
}

// 全局变量
let config = null

// ==================== 工具函数 ====================

// 获取 IP（优先使用 EdgeOne 提供的 eo-connecting-ip）
function getIp(req) {
  // 尝试从各种 HTTP 头中获取 IP 地址
  const ip = req.headers['eo-connecting-ip'] ||
    req.headers['x-forwarded-for']?.split(',')[0]?.trim() ||
    req.headers['x-real-ip'] ||
    req.headers['forwarded']?.match(/for=([^;]+)/)?.[1]?.trim() ||
    req.headers['x-client-ip'] ||
    req.headers['x-cluster-client-ip'] ||
    req.headers['x-forwarded'] ||
    req.headers['forwarded-for'] ||
    req.ip ||
    'unknown'

  // 记录 IP 获取结果
  logger.info('获取 IP 地址:', ip)

  return ip
}

// ==================== Supabase 代理层 ====================

function createSupabaseProxy(req) {
  const applyCommentFilters = (q, query) => {
    let builder = q.eq('type', 'comment')

    if (query.urlIn && query.urlIn.length) {
      builder = builder.in('url', query.urlIn)
    }

    if (query.ridIn && query.ridIn.length) {
      builder = builder.in('rid', query.ridIn)
    }

    if (query.ridIsNullOrEmpty) {
      builder = builder.or('rid.is.null,rid.eq.')
    }

    if (query.topTrue) {
      builder = builder.eq('top', true)
    }

    if (query.topFalseOrNull) {
      builder = builder.or('top.is.null,top.eq.false')
    }

    if (query.before) {
      builder = builder.lt('created', query.before)
    }

    if (query.after) {
      builder = builder.gt('created', query.after)
    }

    if (query.ip) {
      builder = builder.eq('ip', query.ip)
    }

    if (query.uidEq) {
      builder = builder.eq('uid', query.uidEq)
    }

    if (query.spamOnly) {
      // 如果只查询审核中的评论
      builder = builder.eq('is_spam', true)
      if (query.uidEq) {
        builder = builder.eq('uid', query.uidEq)
      }
    } else if (query.notSpamOnly) {
      // 只查询非垃圾评论 (is_spam is null or is_spam = false)
      builder = builder.or('is_spam.is.null,is_spam.eq.false')
    } else if (query.includeSpam === false) {
      // 普通用户：显示非垃圾评论，以及自己的垃圾评论
      // 使用 uid 匹配
      if (query.uid) {
        builder = builder.or(
          `is_spam.is.null,is_spam.eq.false,and(is_spam.eq.true,uid.eq.${query.uid})`
        )
      } else {
        builder = builder.or('is_spam.is.null,is_spam.eq.false')
      }
    }

    if (query.keyword) {
      const safeKeyword = query.keyword
        .replace(/[,]/g, ' ')
        .replace(/[%*]/g, ' ')
        .trim()
      if (safeKeyword) {
        builder = builder.or(
          `nick.ilike.*${safeKeyword}*,mail.ilike.*${safeKeyword}*,link.ilike.*${safeKeyword}*,ip.ilike.*${safeKeyword}*,comment.ilike.*${safeKeyword}*,url.ilike.*${safeKeyword}*,href.ilike.*${safeKeyword}*`
        )
      }
    }

    return builder
  }

  // 分两次查询：非垃圾评论 + 自己的垃圾评论，然后合并
  async function getCommentsWithSpam(query) {
    logger.info('[Supabase] 使用合并查询方式获取评论')

    // 查询1：获取所有非垃圾评论
    const notSpamQuery = { ...query, notSpamOnly: true }
    delete notSpamQuery.includeSpam
    delete notSpamQuery.nick
    delete notSpamQuery.mail

    let builder1 = applyCommentFilters(
      supabase.from('twikoo').select('*'),
      notSpamQuery
    )

    if (query.orderByCreatedDesc) {
      builder1 = builder1.order('created', { ascending: false })
    }

    if (query.range) {
      builder1 = builder1.range(query.range.from, query.range.to)
    } else if (query.limit) {
      builder1 = builder1.limit(query.limit)
    }

    // 查询2：获取自己的垃圾评论（使用昵称和邮箱匹配）
    const spamQuery = {
      ...query,
      spamOnly: true,
      nick: query.nick,
      mail: query.mail
    }
    delete spamQuery.includeSpam
    delete spamQuery.notSpamOnly

    let builder2 = applyCommentFilters(
      supabase.from('twikoo').select('*'),
      spamQuery
    )

    if (query.orderByCreatedDesc) {
      builder2 = builder2.order('created', { ascending: false })
    }

    // 并行执行两个查询
    const [{ data: notSpamData, error: error1 }, { data: spamData, error: error2 }] = await Promise.all([
      builder1,
      builder2
    ])

    if (error1) {
      logger.error('[Supabase] 获取非垃圾评论失败:', error1.message)
      throw error1
    }
    if (error2) {
      logger.error('[Supabase] 获取垃圾评论失败:', error2.message)
      throw error2
    }

    // 合并结果并去重
    const notSpamComments = notSpamData || []
    const spamComments = spamData || []
    const allComments = [...notSpamComments]

    // 只添加不在非垃圾评论列表中的垃圾评论
    const existingIds = new Set(notSpamComments.map(c => c.id))
    for (const spamComment of spamComments) {
      if (!existingIds.has(spamComment.id)) {
        allComments.push(spamComment)
      }
    }

    // 按时间排序
    if (query.orderByCreatedDesc) {
      allComments.sort((a, b) => new Date(b.created) - new Date(a.created))
    }

    // 应用 limit
    let resultComments = allComments
    if (query.limit && resultComments.length > query.limit) {
      resultComments = resultComments.slice(0, query.limit)
    }

    logger.info('[Supabase] getCommentsWithSpam 查询结果:', {
      notSpamCount: notSpamComments.length,
      spamCount: spamComments.length,
      totalCount: resultComments.length
    })

    // 转换字段名以保持与原始代码兼容
    return resultComments.map(item => ({
      ...item,
      mailMd5: item.mail_md5,
      isSpam: item.is_spam === true,
      like: item.likes
    }))
  }

  // 使用 uid 从数据库查询用户的昵称和邮箱
  async function getUserInfoByUid(uid) {
    if (!uid) return null
    try {
      const { data, error } = await supabase
        .from('twikoo')
        .select('nick, mail')
        .eq('type', 'comment')
        .eq('uid', uid)
        .order('created', { ascending: false })
        .limit(1)

      if (error) {
        logger.warn('[Supabase] 查询用户信息失败:', error.message)
        return null
      }

      // data 是数组，取第一个元素
      if (data && data.length > 0) {
        logger.info('[Supabase] 查询到用户信息:', { nick: data[0].nick, mail: data[0].mail })
        return { nick: data[0].nick, mail: data[0].mail }
      }
      return null
    } catch (e) {
      logger.error('[Supabase] 获取用户信息异常:', e.message)
      return null
    }
  }

  // 使用 uid 查询自己的垃圾评论（兼容旧逻辑）
  async function getCommentsWithSpamByUid(query) {
    logger.info('[Supabase] 使用 uid 合并查询方式获取评论')

    // 查询1：获取所有非垃圾评论
    const notSpamQuery = { ...query, notSpamOnly: true }
    delete notSpamQuery.includeSpam

    let builder1 = applyCommentFilters(
      supabase.from('twikoo').select('*'),
      notSpamQuery
    )

    if (query.orderByCreatedDesc) {
      builder1 = builder1.order('created', { ascending: false })
    }

    if (query.range) {
      builder1 = builder1.range(query.range.from, query.range.to)
    } else if (query.limit) {
      builder1 = builder1.limit(query.limit)
    }

    // 查询2：获取自己的垃圾评论（使用 uid 匹配）
    const spamQuery = {
      ...query,
      spamOnly: true,
      uidEq: query.uid
    }
    delete spamQuery.includeSpam
    delete spamQuery.notSpamOnly

    let builder2 = applyCommentFilters(
      supabase.from('twikoo').select('*'),
      spamQuery
    )

    if (query.orderByCreatedDesc) {
      builder2 = builder2.order('created', { ascending: false })
    }

    // 并行执行两个查询
    const [{ data: notSpamData, error: error1 }, { data: spamData, error: error2 }] = await Promise.all([
      builder1,
      builder2
    ])

    if (error1) {
      logger.error('[Supabase] 获取非垃圾评论失败:', error1.message)
      throw error1
    }
    if (error2) {
      logger.error('[Supabase] 获取垃圾评论失败:', error2.message)
      throw error2
    }

    // 合并结果并去重
    const notSpamComments = notSpamData || []
    const spamComments = spamData || []
    const allComments = [...notSpamComments]

    // 只添加不在非垃圾评论列表中的垃圾评论
    const existingIds = new Set(notSpamComments.map(c => c.id))
    for (const spamComment of spamComments) {
      if (!existingIds.has(spamComment.id)) {
        allComments.push(spamComment)
      }
    }

    // 按时间排序
    if (query.orderByCreatedDesc) {
      allComments.sort((a, b) => new Date(b.created) - new Date(a.created))
    }

    // 应用 limit
    let resultComments = allComments
    if (query.limit && resultComments.length > query.limit) {
      resultComments = resultComments.slice(0, query.limit)
    }

    logger.info('[Supabase] getCommentsWithSpamByUid 查询结果:', {
      notSpamCount: notSpamComments.length,
      spamCount: spamComments.length,
      totalCount: resultComments.length
    })

    // 转换字段名以保持与原始代码兼容
    return resultComments.map(item => ({
      ...item,
      mailMd5: item.mail_md5,
      isSpam: item.is_spam === true,
      like: item.likes
    }))
  }

  return {
    async getComments(query = {}) {
      logger.info('[Supabase] getComments 查询参数:', {
        urlIn: query.urlIn,
        includeSpam: query.includeSpam,
        uid: query.uid,
        ridIn: query.ridIn,
        ridIsNullOrEmpty: query.ridIsNullOrEmpty
      })

      // 特殊处理：普通用户需要查询自己的审核中评论时，使用两次查询合并结果
      if (query.includeSpam === false && !query.spamOnly && query.uid) {
        return await getCommentsWithSpamByUid(query)
      }

      let builder = applyCommentFilters(
        supabase.from('twikoo').select('*'),
        query
      )

      if (query.orderByCreatedDesc) {
        builder = builder.order('created', { ascending: false })
      }

      if (query.range) {
        builder = builder.range(query.range.from, query.range.to)
      } else if (query.limit) {
        builder = builder.limit(query.limit)
      }

      const { data, error } = await builder

      if (error) {
        logger.error('[Supabase] 获取评论失败:', error.message)
        throw error
      }

      logger.info('[Supabase] getComments 查询结果数量:', data ? data.length : 0)

      // 转换字段名以保持与原始代码兼容
      logger.info('查询到的评论数据:', data.map(item => ({ id: item.id, is_spam: item.is_spam, is_spam_type: typeof item.is_spam, nick: item.nick })))
      return data.map(item => ({
        ...item,
        mailMd5: item.mail_md5,
        isSpam: item.is_spam === true, // 确保是布尔值
        like: item.likes
      }))
    },
    async countComments(query = {}) {
      const { count, error } = await applyCommentFilters(
        supabase.from('twikoo').select('id', { count: 'exact', head: true }),
        query
      )

      if (error) {
        logger.error('[Supabase] 统计评论失败:', error.message)
        throw error
      }

      return count || 0
    },
    async addComment(comment) {
      // 转换字段名以匹配数据库结构
      const dbComment = {
        ...comment,
        type: 'comment',
        mail_md5: comment.mailMd5 || comment.mail_md5,
        is_spam: comment.isSpam !== undefined ? comment.isSpam : comment.is_spam,
        likes: comment.like || comment.likes || []
      }

      // 删除不需要的字段
      delete dbComment.mailMd5
      delete dbComment.isSpam
      delete dbComment.like

      logger.info('addComment - 准备插入数据库，is_spam:', dbComment.is_spam, 'type:', typeof dbComment.is_spam)

      const { data, error } = await supabase
        .from('twikoo')
        .insert(dbComment)
        .select('id')
        .single()

      if (error) {
        logger.error('[Supabase] 添加评论失败:', error.message)
        throw error
      }

      return { id: data.id }
    },
    async updateComment(id, updates) {
      // 转换字段名以匹配数据库结构
      const dbUpdates = { ...updates }

      if (updates.mailMd5 !== undefined) {
        dbUpdates.mail_md5 = updates.mailMd5
        delete dbUpdates.mailMd5
      }

      if (updates.isSpam !== undefined) {
        dbUpdates.is_spam = updates.isSpam
        delete dbUpdates.isSpam
      }

      if (updates.like !== undefined) {
        dbUpdates.likes = updates.like
        delete dbUpdates.like
      }

      const { error } = await supabase
        .from('twikoo')
        .update(dbUpdates)
        .eq('id', id)
        .eq('type', 'comment')

      if (error) {
        logger.error('[Supabase] 更新评论失败:', error.message)
        throw error
      }

      return { updated: 1 }
    },
    async deleteComment(id) {
      // 删除父评论时一并删除其子评论（rid 指向父评论 id）
      const { error } = await supabase
        .from('twikoo')
        .delete()
        .or(`id.eq.${id},rid.eq.${id}`)
        .eq('type', 'comment')

      if (error) {
        logger.error('[Supabase] 删除评论失败:', error.message)
        throw error
      }

      return { deleted: 1 }
    },
    async getComment(id) {
      const { data, error } = await supabase
        .from('twikoo')
        .select('*')
        .eq('id', id)
        .eq('type', 'comment')
        .single()

      if (error) {
        if (error.code === 'PGRST116') {
          return null
        }
        logger.error('[Supabase] 获取评论失败:', error.message)
        throw error
      }

      // 转换字段名以保持与原始代码兼容
      return {
        ...data,
        mailMd5: data.mail_md5,
        isSpam: data.is_spam,
        like: data.likes
      }
    },
    async bulkAddComments(comments) {
      const commentsWithType = comments.map(comment => {
        // 转换字段名以匹配数据库结构
        const dbComment = {
          ...comment,
          type: 'comment',
          mail_md5: comment.mailMd5,
          is_spam: comment.isSpam,
          likes: comment.like
        }

        // 删除不需要的字段
        delete dbComment.mailMd5
        delete dbComment.isSpam
        delete dbComment.like

        return dbComment
      })

      const { error } = await supabase
        .from('twikoo')
        .insert(commentsWithType)

      if (error) {
        logger.error('[Supabase] 批量添加评论失败:', error.message)
        throw error
      }

      return { inserted: comments.length }
    },
    async getConfig() {
      const { data, error } = await supabase
        .from('twikoo')
        .select('config')
        .eq('type', 'config')
        .single()

      if (error) {
        if (error.code === 'PGRST116') {
          return {}
        }
        logger.error('[Supabase] 获取配置失败:', error.message)
        throw error
      }

      return data.config || {}
    },
    async saveConfig(newConfig) {
      // 先尝试更新配置
      const { error: updateError } = await supabase
        .from('twikoo')
        .update({ config: newConfig })
        .eq('type', 'config')

      if (updateError) {
        // 如果更新失败（可能是因为配置不存在），则插入新配置
        const { error: insertError } = await supabase
          .from('twikoo')
          .insert({
            type: 'config',
            config: newConfig
          })

        if (insertError) {
          logger.error('[Supabase] 保存配置失败:', insertError.message)
          throw insertError
        }
      }

      return { saved: 1 }
    },
    async getCounter(url) {
      const { data, error } = await supabase
        .from('twikoo')
        .select('*')
        .eq('type', 'counter')
        .eq('url', url)
        .single()

      if (error) {
        if (error.code === 'PGRST116') {
          return null
        }
        logger.error('[Supabase] 获取计数器失败:', error.message)
        throw error
      }

      return data
    },
    async incCounter(url, title) {
      // 先尝试获取计数器
      const counter = await this.getCounter(url)

      if (counter) {
        // 如果计数器存在，则更新计数
        const { data, error } = await supabase
          .from('twikoo')
          .update({
            count: (counter.count || 0) + 1,
            updated: Date.now()
          })
          .eq('id', counter.id)
          .select('*')
          .single()

        if (error) {
          logger.error('[Supabase] 更新计数器失败:', error.message)
          throw error
        }

        return data
      } else {
        // 如果计数器不存在，则创建新计数器
        const { data, error } = await supabase
          .from('twikoo')
          .insert({
            type: 'counter',
            url: url,
            title: title,
            count: 1,
            created: Date.now(),
            updated: Date.now()
          })
          .select('*')
          .single()

        if (error) {
          logger.error('[Supabase] 创建计数器失败:', error.message)
          throw error
        }

        return data
      }
    },
    // 使用 uid 从数据库查询用户的昵称和邮箱
    async getUserInfoByUid(uid) {
      if (!uid) return null
      try {
        const { data, error } = await supabase
          .from('twikoo')
          .select('nick, mail')
          .eq('type', 'comment')
          .eq('uid', uid)
          .order('created', { ascending: false })
          .limit(1)

        if (error) {
          logger.warn('[Supabase] 查询用户信息失败:', error.message)
          return null
        }

        // data 是数组，取第一个元素
        if (data && data.length > 0) {
          logger.info('[Supabase] 查询到用户信息:', { nick: data[0].nick, mail: data[0].mail })
          return { nick: data[0].nick, mail: data[0].mail }
        }
        return null
      } catch (e) {
        logger.error('[Supabase] 获取用户信息异常:', e.message)
        return null
      }
    }
  }
}

// ==================== 配置管理 ====================

async function readConfig(req) {
  try {
    const db = createSupabaseProxy(req)
    config = await db.getConfig()

    // 验证关键配置项
    if (config.AKISMET_KEY) {
      logger.info('当前 AKISMET_KEY:', config.AKISMET_KEY)
    }
    if (config.LIMIT_LENGTH) {
      logger.info('当前 LIMIT_LENGTH:', config.LIMIT_LENGTH)
    }
    if (config.FORBIDDEN_WORDS) {
      logger.info('当前 FORBIDDEN_WORDS:', config.FORBIDDEN_WORDS)
    }
    if (config.BLOCKED_WORDS) {
      logger.info('当前 BLOCKED_WORDS:', config.BLOCKED_WORDS)
    }
  } catch (e) {
    logger.error('读取配置失败:', e.message)
    config = {}
  }
  return config
}

async function writeConfig(db, newConfig) {
  if (!Object.keys(newConfig).length) return 0
  logger.info('写入配置')

  // 先读取现有配置
  const existingConfig = await db.getConfig()
  logger.info('现有配置:', JSON.stringify(existingConfig))
  logger.info('新配置:', JSON.stringify(newConfig))

  // 合并配置，保留现有配置项
  const mergedConfig = {
    ...existingConfig,
    ...newConfig
  }
  logger.info('合并后配置:', JSON.stringify(mergedConfig))

  // 验证关键配置项
  if (mergedConfig.AKISMET_KEY) {
    logger.info('AKISMET_KEY 配置值为:', mergedConfig.AKISMET_KEY)
  }
  if (mergedConfig.LIMIT_LENGTH) {
    logger.info('LIMIT_LENGTH 配置值为:', mergedConfig.LIMIT_LENGTH)
  }
  if (mergedConfig.FORBIDDEN_WORDS) {
    logger.info('FORBIDDEN_WORDS 配置值为:', mergedConfig.FORBIDDEN_WORDS)
  }
  if (mergedConfig.BLOCKED_WORDS) {
    logger.info('BLOCKED_WORDS 配置值为:', mergedConfig.BLOCKED_WORDS)
  }

  await db.saveConfig(mergedConfig)

  // 验证保存是否成功
  const savedConfig = await db.getConfig()
  logger.info('保存后从数据库读取的配置:', JSON.stringify(savedConfig))

  config = null
  return 1
}

async function isAdmin(accessToken, req) {
  // 如果 accessToken 为空，直接返回 false
  if (!accessToken) {
    logger.info('isAdmin: accessToken 为空，返回 false')
    return false
  }

  // 如果全局配置存在且包含管理员密码，直接使用
  if (config && config.ADMIN_PASS) {
    const isAdmin = config.ADMIN_PASS === md5(accessToken)
    logger.info('isAdmin: 使用全局配置判断，accessToken:', accessToken.substring(0, 10) + '...', 'isAdmin:', isAdmin)
    return isAdmin
  }

  // 否则从数据库读取配置
  try {
    const db = createSupabaseProxy(req)
    const dbConfig = await db.getConfig()

    // 更新全局配置
    if (dbConfig.ADMIN_PASS) {
      config = dbConfig
    }

    return dbConfig.ADMIN_PASS === md5(accessToken)
  } catch (e) {
    logger.error('检查管理员权限失败:', e.message)
    return false
  }
}

// ==================== 密码管理 ====================

async function setPassword(event, db, accessToken, req) {
  const isAdminUser = await isAdmin(accessToken, req)
  if (config.ADMIN_PASS && !isAdminUser) {
    return { code: RES_CODE.PASS_EXIST, message: '请先登录再修改密码' }
  }
  const ADMIN_PASS = md5(event.password)
  await writeConfig(db, { ADMIN_PASS })
  return { code: RES_CODE.SUCCESS }
}

async function login(password, req) {
  try {
    const db = createSupabaseProxy(req)
    const dbConfig = await db.getConfig()

    if (!Object.keys(dbConfig).length) {
      return { code: RES_CODE.CONFIG_NOT_EXIST, message: '数据库无配置' }
    }
    if (!dbConfig.ADMIN_PASS) {
      return { code: RES_CODE.PASS_NOT_EXIST, message: '未配置管理密码' }
    }
    if (dbConfig.ADMIN_PASS !== md5(password)) {
      return { code: RES_CODE.PASS_NOT_MATCH, message: '密码错误' }
    }
    return { code: RES_CODE.SUCCESS }
  } catch (e) {
    logger.error('登录验证失败:', e.message)
    return { code: RES_CODE.ERROR, message: '登录验证失败' }
  }
}

// ==================== 评论读取 ====================

async function commentGet(event, db, accessToken, req) {
  const res = {}
  try {
    validate(event, ['url'])
    const uid = accessToken
    const limit = parseInt(config.COMMENT_PAGE_SIZE) || 8
    let more = false

    const urlQuery = getUrlQuery(event.url)

    // 检查管理员权限
    const isAdminUser = await isAdmin(accessToken, req)

    // 计算总数（仅主楼，包含当前用户可见范围）
    let count = 0
    if (isAdminUser) {
      count = await db.countComments({
        urlIn: urlQuery,
        ridIsNullOrEmpty: true
      })
    } else {
      const countNotSpam = await db.countComments({
        urlIn: urlQuery,
        ridIsNullOrEmpty: true,
        notSpamOnly: true
      })
      // 使用 uid 来统计用户自己的审核中评论
      const countOwnSpam = await db.countComments({
        urlIn: urlQuery,
        ridIsNullOrEmpty: true,
        spamOnly: true,
        uidEq: uid
      })
      count = countNotSpam + countOwnSpam
    }

    // 获取主楼评论（分页）
    let mainComments = await db.getComments({
      urlIn: urlQuery,
      ridIsNullOrEmpty: true,
      includeSpam: isAdminUser,
      uid: uid,
      before: event.before,
      orderByCreatedDesc: true,
      limit: limit + 1
    })

    // 处理置顶和分页
    let top = []
    if (!config.TOP_DISABLED && !event.before) {
      top = mainComments.filter(c => c.top === true)
      mainComments = mainComments.filter(c => c.top !== true)
    }

    if (mainComments.length > limit) {
      more = true
      mainComments = mainComments.slice(0, limit)
    }

    // 合并置顶
    mainComments = [...top, ...mainComments]

    // 获取回复（仅当前主楼）
    const mainIds = mainComments.map(c => c.id)
    const replies = mainIds.length
      ? (await db.getComments({
        ridIn: mainIds,
        includeSpam: isAdminUser,
        uid: uid
      }))
      : []

    const allComments = [...mainComments, ...replies]
    logger.info('commentGet - 评论数据（转换前）:', allComments.map(c => ({ id: c.id, isSpam: c.isSpam, nick: c.nick })))
    // 使用 uid 来验证审核中评论的归属
    res.data = parseComment(allComments, uid, config, isAdminUser)
    logger.info('commentGet - 评论数据（转换后）:', res.data.map(c => ({ id: c.id, isSpam: c.isSpam, nick: c.nick })))
    res.more = more
    res.count = count
  } catch (e) {
    res.data = []
    res.message = e.message
  }
  return res
}

// ==================== 管理员评论操作 ====================

async function commentGetForAdmin(event, db, accessToken, req) {
  const res = {}
  const isAdminUser = await isAdmin(accessToken, req)
  if (isAdminUser) {
    validate(event, ['per', 'page'])

    const query = {
      orderByCreatedDesc: true,
      keyword: event.keyword ? event.keyword.toLowerCase() : null
    }

    if (event.type === 'VISIBLE') {
      query.notSpamOnly = true
    } else if (event.type === 'HIDDEN') {
      query.spamOnly = true
    }

    const count = await db.countComments(query)
    const start = event.per * (event.page - 1)
    const data = await db.getComments({
      ...query,
      range: { from: start, to: start + event.per - 1 }
    })

    res.code = RES_CODE.SUCCESS
    res.count = count
    res.data = parseCommentForAdmin(data)
  } else {
    res.code = RES_CODE.NEED_LOGIN
    res.message = '请先登录'
  }
  return res
}

async function commentSetForAdmin(event, db, accessToken, req) {
  const res = {}
  const isAdminUser = await isAdmin(accessToken, req)
  if (isAdminUser) {
    const targetId = event.id || event._id
    validate({ ...event, id: targetId }, ['id', 'set'])
    await db.updateComment(targetId, {
      ...event.set,
      updated: Date.now()
    })
    res.code = RES_CODE.SUCCESS
    res.updated = 1
  } else {
    res.code = RES_CODE.NEED_LOGIN
    res.message = '请先登录'
  }
  return res
}

async function commentDeleteForAdmin(event, db, accessToken, req) {
  const res = {}
  const isAdminUser = await isAdmin(accessToken, req)
  if (isAdminUser) {
    const targetId = event.id || event._id
    validate({ ...event, id: targetId }, ['id'])
    await db.deleteComment(targetId)
    res.code = RES_CODE.SUCCESS
    res.deleted = 1
  } else {
    res.code = RES_CODE.NEED_LOGIN
    res.message = '请先登录'
  }
  return res
}

async function commentImportForAdmin(event, db, accessToken, req) {
  const res = {}
  let logText = ''
  const log = (message) => {
    logText += `${new Date().toLocaleString()} ${message}\n`
  }
  const isAdminUser = await isAdmin(accessToken, req)
  if (isAdminUser) {
    try {
      validate(event, ['source', 'file'])
      log(`开始导入 ${event.source}`)
      let comments
      switch (event.source) {
        case 'valine': {
          const valineDb = await readFile(event.file, 'json', log)
          comments = await commentImportValine(valineDb, log)
          break
        }
        case 'disqus': {
          const disqusDb = await readFile(event.file, 'xml', log)
          comments = await commentImportDisqus(disqusDb, log)
          break
        }
        case 'artalk': {
          const artalkDb = await readFile(event.file, 'json', log)
          comments = await commentImportArtalk(artalkDb, log)
          break
        }
        case 'artalk2': {
          const artalkDb = await readFile(event.file, 'json', log)
          comments = await commentImportArtalk2(artalkDb, log)
          break
        }
        case 'twikoo': {
          const twikooDb = await readFile(event.file, 'json', log)
          comments = await commentImportTwikoo(twikooDb, log)
          break
        }
        default:
          throw new Error(`不支持 ${event.source} 的导入，请更新 Twikoo 云函数至最新版本`)
      }
      await db.bulkAddComments(comments)
      log('导入成功')
    } catch (e) {
      log(e.message)
    }
    res.code = RES_CODE.SUCCESS
    res.log = logText
    logger.info(logText)
  } else {
    res.code = RES_CODE.NEED_LOGIN
    res.message = '请先登录'
  }
  return res
}

async function commentExportForAdmin(event, db, accessToken, req) {
  const res = {}
  const isAdminUser = await isAdmin(accessToken, req)
  if (isAdminUser) {
    const data = await db.getComments()
    res.code = RES_CODE.SUCCESS
    res.data = data
  } else {
    res.code = RES_CODE.NEED_LOGIN
    res.message = '请先登录'
  }
  return res
}

async function readFile(file, type, log) {
  try {
    let content = file.toString('utf8')
    log('评论文件读取成功')
    if (type === 'json') {
      content = jsonParse(content)
      log('评论文件 JSON 解析成功')
    } else if (type === 'xml') {
      content = await xml2js.parseStringPromise(content)
      log('评论文件 XML 解析成功')
    }
    return content
  } catch (e) {
    log(`评论文件读取失败：${e.message}`)
  }
}

// ==================== 点赞 ====================

async function commentLike(event, db, accessToken) {
  const res = {}
  const targetId = event.id || event._id
  validate({ ...event, id: targetId }, ['id'])
  const uid = accessToken
  const comment = await db.getComment(targetId)

  if (comment) {
    let likes = comment.likes || []
    const index = likes.indexOf(uid)
    if (index === -1) {
      likes.push(uid)
    } else {
      likes.splice(index, 1)
    }
    await db.updateComment(targetId, { likes: likes })
    res.updated = 1
  } else {
    res.updated = 0
  }
  return res
}

// ==================== 评论提交 ====================

async function commentSubmit(event, req, db, accessToken) {
  const res = {}
  validate(event, ['url', 'ua', 'comment'])

  const ip = getIp(req)

  // 限流检查
  await limitFilter(db, ip)

  // 验证码检查
  await checkCaptcha(event, ip)

  // 解析评论数据
  const data = await parseCommentData(event, req, accessToken, ip)

  // 垃圾检测
  const isSpam = await postCheckSpam(data, config)
  logger.log('垃圾检测结果：', isSpam)

  // 人工审核模式下，允许垃圾评论写入数据库（只有本人和管理员可见）
  // 其他情况下，如果是垃圾评论则阻止提交
  if (isSpam && config.AKISMET_KEY !== 'MANUAL_REVIEW') {
    throw new Error('评论被检测为垃圾评论，请修改后重新提交')
  }

  // 保存评论
  const result = await db.addComment(data)
  data.id = result.id
  res.id = result.id

  // 异步处理通知
  postSubmit(data, db).catch(e => {
    logger.error('POST_SUBMIT 失败', e.message)
  })

  return res
}

async function parseCommentData(event, req, accessToken, ip) {
  const timestamp = Date.now()
  const isAdminUser = await isAdmin(accessToken, req)
  const isBloggerMail = equalsMail(config.BLOGGER_EMAIL, event.mail)

  logger.info('parseCommentData - isAdminUser:', isAdminUser, 'isBloggerMail:', isBloggerMail)

  if (isBloggerMail && !isAdminUser) {
    throw new Error('请先登录管理面板，再使用博主身份发送评论')
  }

  const hashMethod = config.GRAVATAR_CDN === 'cravatar.cn' ? md5 : sha256

  // 记录 preCheckSpam 调用前的配置状态
  logger.info('preCheckSpam 调用前配置:', {
    AKISMET_KEY: config.AKISMET_KEY,
    LIMIT_LENGTH: config.LIMIT_LENGTH,
    FORBIDDEN_WORDS: config.FORBIDDEN_WORDS,
    BLOCKED_WORDS: config.BLOCKED_WORDS,
    isAdminUser: isAdminUser
  })

  const commentDo = {
    id: uuidv4().replace(/-/g, ''),
    uid: accessToken,
    nick: event.nick ? event.nick : '匿名',
    mail: event.mail ? event.mail : '',
    mail_md5: event.mail ? hashMethod(normalizeMail(event.mail)) : '',
    link: event.link ? event.link : '',
    ua: event.ua,
    ip: ip,
    master: isBloggerMail,
    url: event.url,
    href: event.href,
    comment: xss(event.comment),
    pid: event.pid ? event.pid : event.rid,
    rid: event.rid,
    is_spam: isAdminUser ? false : preCheckSpamWithLog(event, config),
    created: timestamp,
    updated: timestamp
  }
  logger.info('commentDo.is_spam:', commentDo.is_spam, 'isAdminUser:', isAdminUser)

  // 处理 QQ 邮箱和头像
  if (isQQ(event.mail)) {
    commentDo.mail = addQQMailSuffix(event.mail)
    commentDo.mail_md5 = md5(normalizeMail(commentDo.mail))
    try {
      commentDo.avatar = await getQQAvatar(event.mail)
    } catch (e) {
      logger.warn('获取 QQ 头像失败：', e.message)
    }
  }

  return commentDo
}

async function postSubmit(comment, db) {
  try {
    logger.log('POST_SUBMIT')

    // 获取父评论
    const getParentComment = async (c) => {
      if (c.pid) {
        return db.getComment(c.pid)
      }
      return null
    }

    // 发送通知
    await sendNotice(comment, config, getParentComment)
  } catch (e) {
    logger.warn('POST_SUBMIT 失败', e)
  }
}

async function limitFilter(db, ip) {
  let limitPerMinute = parseInt(config.LIMIT_PER_MINUTE)
  if (Number.isNaN(limitPerMinute)) limitPerMinute = 10

  if (limitPerMinute) {
    const recentCount = await db.countComments({
      ip: ip,
      after: Date.now() - 600000
    })
    if (recentCount > limitPerMinute) {
      throw new Error('发言频率过高')
    }
  }

  let limitPerMinuteAll = parseInt(config.LIMIT_PER_MINUTE_ALL)
  if (Number.isNaN(limitPerMinuteAll)) limitPerMinuteAll = 10

  if (limitPerMinuteAll) {
    const recentCountAll = await db.countComments({
      after: Date.now() - 600000
    })
    if (recentCountAll > limitPerMinuteAll) {
      throw new Error('评论太火爆啦 >_< 请稍后再试')
    }
  }
}

async function checkCaptcha(event, ip) {
  if (config.TURNSTILE_SITE_KEY && config.TURNSTILE_SECRET_KEY) {
    await checkTurnstileCaptcha({
      ip: ip,
      turnstileToken: event.turnstileToken,
      turnstileTokenSecretKey: config.TURNSTILE_SECRET_KEY
    })
  }
}

// ==================== 配置操作 ====================

async function setConfig(event, db, accessToken, req) {
  const isAdminUser = await isAdmin(accessToken, req)
  if (isAdminUser) {
    await writeConfig(db, event.config)
    return { code: RES_CODE.SUCCESS }
  } else {
    return { code: RES_CODE.NEED_LOGIN, message: '请先登录' }
  }
}

// ==================== 计数器 ====================

async function counterGet(event, db) {
  const res = {}
  try {
    validate(event, ['url'])
    const record = await db.getCounter(event.url)
    res.data = record || {}
    res.time = res.data.time || 0
    res.updated = await db.incCounter(event.url, event.title)
  } catch (e) {
    res.message = e.message
  }
  return res
}

// ==================== 评论统计 ====================

async function getCommentsCount(event, db) {
  const res = {}
  try {
    validate(event, ['urls'])
    const urlsQuery = getUrlsQuery(event.urls)
    const comments = await db.getComments({
      urlIn: urlsQuery,
      notSpamOnly: true,
      ridIsNullOrEmpty: event.includeReply ? false : true
    })

    res.data = []
    for (const url of event.urls) {
      const urlVariants = getUrlQuery(url)
      const count = comments.filter(c =>
        urlVariants.includes(c.url) &&
        (event.includeReply || !c.rid || c.rid === '')
      ).length
      res.data.push({ url, count })
    }
  } catch (e) {
    res.message = e.message
  }
  return res
}

async function getRecentComments(event, db) {
  const res = {}
  try {
    const pageSize = Math.min(event.pageSize || 10, 100)
    const urlsQuery = event.urls && event.urls.length
      ? getUrlsQuery(event.urls)
      : null

    let comments = await db.getComments({
      urlIn: urlsQuery,
      notSpamOnly: true,
      ridIsNullOrEmpty: event.includeReply ? false : true,
      orderByCreatedDesc: true,
      limit: pageSize
    })

    res.data = comments.map(comment => ({
      id: comment.id,
      url: comment.url,
      nick: comment.nick,
      avatar: getAvatar(comment, config),
      mailMd5: getMailMd5(comment),
      link: comment.link,
      comment: comment.comment,
      commentText: comment.comment.replace(/<[^>]*>/g, ''),
      created: comment.created
    }))
  } catch (e) {
    res.message = e.message
  }
  return res
}

// EdgeOne Pages Node Function 入口
export async function onRequest(context) {
  const { request } = context
  const url = new URL(request.url)
  const method = request.method

  // 请求日志
  logRequest(method, url.pathname, url.search)

  // 将 EdgeOne 请求转换为 Express 可处理的格式
  return new Promise(async (resolve) => {
    try {

      // 构造模拟的 req 对象
      const headers = {}
      request.headers.forEach((value, key) => {
        headers[key.toLowerCase()] = value
      })

      let body = null
      if (method === 'POST' || method === 'PUT' || method === 'PATCH') {
        try {
          body = await request.json()
        } catch (e) {
          body = {}
        }
      }

      const req = {
        method,
        url: url.pathname + url.search,
        path: url.pathname,
        headers,
        body,
        ip: headers['x-real-ip'] || headers['x-forwarded-for']?.split(',')[0]?.trim() || 'unknown',
        protocol: url.protocol.replace(':', ''),
        get: (name) => headers[name.toLowerCase()]
      }

      // 构造模拟的 res 对象
      let statusCode = 200
      const resHeaders = {}
      let resBody = null

      const res = {
        status: (code) => { statusCode = code; return res },
        setHeader: (name, value) => { resHeaders[name] = value },
        set: (name, value) => { resHeaders[name] = value },
        json: (data) => {
          resHeaders['Content-Type'] = 'application/json'
          resBody = JSON.stringify(data)
          finish()
        },
        send: (data) => {
          resBody = data
          finish()
        },
        end: () => finish()
      }

      function finish() {
        resolve(new Response(resBody, {
          status: statusCode,
          headers: resHeaders
        }))
      }


      // CORS 处理
      const origin = headers.origin
      if (origin) {
        res.setHeader('Access-Control-Allow-Credentials', 'true')
        res.setHeader('Access-Control-Allow-Origin', origin)
        res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        res.setHeader('Access-Control-Allow-Headers', 'X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version')
        res.setHeader('Access-Control-Max-Age', '600')
      }

      if (method === 'OPTIONS') {
        res.status(204).end()
        return
      }

      if (method === 'GET') {
        res.json({
          code: RES_CODE.SUCCESS,
          message: 'Twikoo 云函数运行正常，请参考 https://twikoo.js.org/frontend.html 完成前端的配置',
          version: VERSION
        })
        return
      }

      if (method === 'POST') {
        // 调用主处理逻辑
        await handlePost(req, res)
        return
      }

      res.status(404).json({ code: 404, message: 'Not Found' })
    } catch (e) {
      logError('onRequest', e)
      resolve(new Response(JSON.stringify({ code: 500, message: e.message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      }))
    }
  })
}

// POST 请求处理主逻辑
async function handlePost(req, res) {
  let accessToken
  const event = req.body || {}
  const ip = getIp(req)

  // 记录事件日志
  logEvent(event.event, ip, {
    url: event.url,
    nick: event.nick,
    id: event.id,
    page: event.page,
    per: event.per
  })

  let result = {}

  try {
    // 生成或使用 accessToken
    accessToken = event.accessToken || uuidv4().replace(/-/g, '')

    // 读取配置
    await readConfig(req)

    // 创建数据库操作对象
    const db = createSupabaseProxy(req)

    switch (event.event) {
      case 'GET_FUNC_VERSION':
        result = getFuncVersion({ VERSION })
        break
      case 'COMMENT_GET':
        result = await commentGet(event, db, accessToken, req)
        break
      case 'COMMENT_GET_FOR_ADMIN':
        result = await commentGetForAdmin(event, db, accessToken, req)
        break
      case 'COMMENT_SET_FOR_ADMIN':
        result = await commentSetForAdmin(event, db, accessToken, req)
        break
      case 'COMMENT_DELETE_FOR_ADMIN':
        result = await commentDeleteForAdmin(event, db, accessToken, req)
        break
      case 'COMMENT_IMPORT_FOR_ADMIN':
        result = await commentImportForAdmin(event, db, accessToken, req)
        break
      case 'COMMENT_LIKE':
        result = await commentLike(event, db, accessToken)
        break
      case 'COMMENT_SUBMIT':
        result = await commentSubmit(event, req, db, accessToken)
        break
      case 'COUNTER_GET':
        result = await counterGet(event, db)
        break
      case 'GET_PASSWORD_STATUS':
        result = await getPasswordStatus(config, VERSION)
        break
      case 'SET_PASSWORD':
        result = await setPassword(event, db, accessToken, req)
        break
      case 'GET_CONFIG':
        const isAdminForConfig = await isAdmin(accessToken, req)
        result = await getConfig({ config, VERSION, isAdmin: isAdminForConfig })
        break
      case 'GET_CONFIG_FOR_ADMIN':
        const isAdminForConfigAdmin = await isAdmin(accessToken, req)
        result = await getConfigForAdmin({ config, isAdmin: isAdminForConfigAdmin })
        break
      case 'SET_CONFIG':
        result = await setConfig(event, db, accessToken, req)
        break
      case 'LOGIN':
        result = await login(event.password, req)
        break
      case 'GET_COMMENTS_COUNT':
        result = await getCommentsCount(event, db)
        break
      case 'GET_RECENT_COMMENTS':
        result = await getRecentComments(event, db)
        break
      case 'EMAIL_TEST':
        const isAdminForEmailTest = await isAdmin(accessToken, req)
        result = await emailTest(event, config, isAdminForEmailTest)
        break
      case 'UPLOAD_IMAGE':
        result = await uploadImage(event, config)
        break
      case 'COMMENT_EXPORT_FOR_ADMIN':
        result = await commentExportForAdmin(event, db, accessToken, req)
        break
      default:
        if (event.event) {
          result.code = RES_CODE.EVENT_NOT_EXIST
          result.message = '请更新 Twikoo 云函数至最新版本'
        } else {
          result.code = RES_CODE.NO_PARAM
          result.message = 'Twikoo 云函数运行正常，请参考 https://twikoo.js.org/frontend.html 完成前端的配置'
          result.version = VERSION
        }
    }

    if (!result.code && !event.accessToken) {
      result.accessToken = accessToken
    }
  } catch (e) {
    logger.error('Twikoo 遇到错误：', e.message, e.stack)
    result.code = RES_CODE.FAIL
    result.message = e.message
  }

  logResponse(event.event, result.code, { count: result.count })
  res.json(result)
}

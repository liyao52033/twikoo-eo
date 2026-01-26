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
  preCheckSpam,
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
import { postCheckSpam } from 'twikoo-func/utils/spam'
import { sendNotice, emailTest } from 'twikoo-func/utils/notify'
import { uploadImage } from 'twikoo-func/utils/image'
import constants from 'twikoo-func/utils/constants'

const { RES_CODE } = constants
const VERSION = '1.6.44'

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
  const comment = comments.find((item) => item._id === pid)
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
    id: comment._id.toString(),
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
    isSpam: comment.is_spam,
    created: comment.created,
    updated: comment.updated
  }
}

/**
 * 筛除隐私字段，拼接回复列表（本地实现，使用自己的 IP 归属地查询）
 */
function parseComment(comments, uid, cfg) {
  const result = []
  for (const comment of comments) {
    if (!comment.rid) {
      const replies = comments
        .filter((item) => item.rid === comment._id.toString())
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
  return {
    async getComments(query = {}) {
      const { data, error } = await supabase
        .from('twikoo')
        .select('*')
        .eq('type', 'comment')

      if (error) {
        logger.error('[Supabase] 获取评论失败:', error.message)
        throw error
      }

      // 转换字段名以保持与原始代码兼容
      return data.map(item => ({
        ...item,
        mailMd5: item.mail_md5,
        isSpam: item.is_spam,
        like: item.likes
      }))
    },
    async countComments(query = {}) {
      const comments = await this.getComments(query)
      return comments.length
    },
    async addComment(comment) {
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
        .eq('_id', id)
        .eq('type', 'comment')

      if (error) {
        logger.error('[Supabase] 更新评论失败:', error.message)
        throw error
      }

      return { updated: 1 }
    },
    async deleteComment(id) {
      // 直接使用 _id 字段执行删除操作，因为 _id 存储的是 UUID 格式的字符串，与前端传递的 ID 格式一致
      const { error } = await supabase
        .from('twikoo')
        .delete()
        .eq('_id', id)
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
        .eq('_id', id)
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
    }
  }
}

// ==================== 配置管理 ====================

async function readConfig(req) {
  try {
    const db = createSupabaseProxy(req)
    config = await db.getConfig()
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

  // 合并配置，保留现有配置项
  const mergedConfig = {
    ...existingConfig,
    ...newConfig
  }

  await db.saveConfig(mergedConfig)
  config = null
  return 1
}

async function isAdmin(accessToken, req) {
  // 如果全局配置存在且包含管理员密码，直接使用
  if (config && config.ADMIN_PASS) {
    return config.ADMIN_PASS === md5(accessToken)
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

    // 获取所有评论
    let allComments = await db.getComments()

    // 过滤主楼评论
    let mainComments = allComments.filter(c =>
      urlQuery.includes(c.url) &&
      (!c.rid || c.rid === '') &&
      (c.isSpam !== true || c.uid === uid || isAdminUser)
    )

    // 计算总数
    const count = mainComments.length

    // 排序
    mainComments.sort((a, b) => b.created - a.created)

    // 处理置顶和分页
    let top = []
    if (!config.TOP_DISABLED && !event.before) {
      top = mainComments.filter(c => c.top === true)
      mainComments = mainComments.filter(c => c.top !== true)
    }

    // 分页
    if (event.before) {
      mainComments = mainComments.filter(c => c.created < event.before)
    }

    if (mainComments.length > limit) {
      more = true
      mainComments = mainComments.slice(0, limit)
    }

    // 合并置顶
    mainComments = [...top, ...mainComments]

    // 获取回复
    const mainIds = mainComments.map(c => c._id)
    const replies = allComments.filter(c =>
      mainIds.includes(c.rid) &&
      (c.isSpam !== true || c.uid === uid || isAdminUser)
    )

    res.data = parseComment([...mainComments, ...replies], uid, config)
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

    let comments = await db.getComments()

    if (event.type === 'VISIBLE') {
      comments = comments.filter(c => c.isSpam !== true)
    } else if (event.type === 'HIDDEN') {
      comments = comments.filter(c => c.isSpam === true)
    }

    if (event.keyword) {
      const keyword = event.keyword.toLowerCase()
      comments = comments.filter(c =>
        (c.nick && c.nick.toLowerCase().includes(keyword)) ||
        (c.mail && c.mail.toLowerCase().includes(keyword)) ||
        (c.link && c.link.toLowerCase().includes(keyword)) ||
        (c.ip && c.ip.toLowerCase().includes(keyword)) ||
        (c.comment && c.comment.toLowerCase().includes(keyword)) ||
        (c.url && c.url.toLowerCase().includes(keyword)) ||
        (c.href && c.href.toLowerCase().includes(keyword))
      )
    }

    comments.sort((a, b) => b.created - a.created)

    const count = comments.length
    const start = event.per * (event.page - 1)
    const data = comments.slice(start, start + event.per)

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
    validate(event, ['id', 'set'])
    await db.updateComment(event.id, {
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
    validate(event, ['id'])
    await db.deleteComment(event.id)
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
  validate(event, ['id'])
  const uid = accessToken
  const comment = await db.getComment(event.id)

  if (comment) {
    let likes = comment.likes || []
    const index = likes.indexOf(uid)
    if (index === -1) {
      likes.push(uid)
    } else {
      likes.splice(index, 1)
    }
    await db.updateComment(event.id, { likes: likes })
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
  if (isSpam) {
    throw new Error('评论被检测为垃圾评论，请修改后重新提交')
  }

  // 保存评论
  const result = await db.addComment(data)
  data.id = result.id
  data._id = result.id
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

  if (isBloggerMail && !isAdminUser) {
    throw new Error('请先登录管理面板，再使用博主身份发送评论')
  }

  const hashMethod = config.GRAVATAR_CDN === 'cravatar.cn' ? md5 : sha256

  const commentDo = {
    _id: uuidv4().replace(/-/g, ''),
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
    is_spam: isAdminUser ? false : preCheckSpam(event, config),
    created: timestamp,
    updated: timestamp
  }

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
    const comments = await db.getComments()
    const recentComments = comments.filter(c =>
      c.ip === ip && c.created > Date.now() - 600000
    )
    if (recentComments.length > limitPerMinute) {
      throw new Error('发言频率过高')
    }
  }

  let limitPerMinuteAll = parseInt(config.LIMIT_PER_MINUTE_ALL)
  if (Number.isNaN(limitPerMinuteAll)) limitPerMinuteAll = 10

  if (limitPerMinuteAll) {
    const comments = await db.getComments()
    const recentComments = comments.filter(c => c.created > Date.now() - 600000)
    if (recentComments.length > limitPerMinuteAll) {
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
    const comments = await db.getComments()

    res.data = []
    for (const url of event.urls) {
      const urlVariants = getUrlQuery(url)
      const count = comments.filter(c =>
        urlVariants.includes(c.url) &&
        c.isSpam !== true &&
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
    let comments = await db.getComments()

    comments = comments.filter(c => c.isSpam !== true)

    if (event.urls && event.urls.length) {
      const urlsQuery = getUrlsQuery(event.urls)
      comments = comments.filter(c => urlsQuery.includes(c.url))
    }

    if (!event.includeReply) {
      comments = comments.filter(c => !c.rid || c.rid === '')
    }

    comments.sort((a, b) => b.created - a.created)

    const pageSize = Math.min(event.pageSize || 10, 100)
    comments = comments.slice(0, pageSize)

    res.data = comments.map(comment => ({
      id: comment._id,
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

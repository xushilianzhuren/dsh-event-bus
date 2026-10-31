/**
 * 事件总线 · 插件（Host 侧，零依赖，无构建）
 *
 * 宪法写"自动执行只有两态：事件触发／令文触发，禁 cron"。但旧执行件
 * scripts/事件触发器.py 把根写死 <workspace>（本机无此目录），
 * 事件台账 89021 条停在 2026-10-18——**总线事实上自那天起停摆**。
 *
 * 本插件把总线原生做进 Host：定时轮询事件源（追加／新文件／mtime），命中规则即派发。
 * 安全边界（比旧件更紧）：
 *   ①只认规则里的 `argv` 数组，不碰 `命令` 字符串——**No shell**，无重定向/管道注入；
 *   ②argv[0] 必须是 python3，argv[1] 必须落在本工作区 scripts/ 下；
 *   ③规则标 `启用:false` 一律不派发；
 *   ④启动第一轮只建基线不派发，避免开机即风暴；
 *   ⑤失败回流：失败事件 24h 去重 + issue log立案。
 *
 * @module dsh-event-bus
 */

import { existsSync } from 'node:fs'
import { readFile, stat, readdir, writeFile, rename } from 'node:fs/promises'
import { execFile } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

export const name = 'ql-bus'

export const inject = ['tools']

const DEFAULTS = Object.freeze({ pollSeconds: 20, autoDispatch: true, enabled: true })

function normalizeConfig(config) {
  const raw = config ?? {}
  if (raw.enabled !== undefined && typeof raw.enabled !== 'boolean') {
    throw new Error('ql-bus: config.enabled must be a boolean')
  }
  if (raw.autoDispatch !== undefined && typeof raw.autoDispatch !== 'boolean') {
    throw new Error('ql-bus: config.autoDispatch must be a boolean')
  }
  if (raw.pollSeconds !== undefined
    && (typeof raw.pollSeconds !== 'number' || !Number.isFinite(raw.pollSeconds) || raw.pollSeconds < 5)) {
    throw new Error('ql-bus: config.pollSeconds must be a finite number >= 5')
  }
  if (raw.root !== undefined && (typeof raw.root !== 'string' || raw.root.trim() === '')) {
    throw new Error('ql-bus: config.root must be a non-empty path string')
  }
  return {
    pollSeconds: raw.pollSeconds ?? DEFAULTS.pollSeconds,
    autoDispatch: raw.autoDispatch ?? DEFAULTS.autoDispatch,
    root: raw.root === undefined ? undefined : raw.root.trim(),
    enabled: raw.enabled ?? DEFAULTS.enabled,
  }
}

function isWorkspace(dir) {
  try {
    return existsSync(join(dir, 'AGENTS.md'))
      && existsSync(join(dir, 'scripts'))
      && (existsSync(join(dir, 'SOUL.md')) || existsSync(join(dir, 'USER.md')))
  } catch {
    return false
  }
}

function findRoot(configured) {
  const candidates = []
  if (configured !== undefined) candidates.push(configured)
  try {
    candidates.push(dirname(dirname(fileURLToPath(import.meta.url))))
  } catch {
    /* 非文件装法 */
  }
  let dir = process.cwd()
  for (let hop = 0; hop < 6; hop += 1) {
    candidates.push(dir)
    const parent = dirname(dir)
    if (parent === dir) break
    dir = parent
  }
  candidates.push('~//Smart Agentharness')
  for (const candidate of candidates) {
    if (isWorkspace(candidate)) return candidate
  }
  return candidates[0]
}

function runFile(cmd, args, cwd, timeoutMs) {
  return new Promise(resolve => {
    execFile(cmd, args, { cwd, timeout: timeoutMs, maxBuffer: 4 * 1024 * 1024 }, (error, stdout, stderr) => {
      resolve({
        ok: error === null,
        code: error === null ? 0 : (typeof error.code === 'number' ? error.code : 1),
        out: `${stdout ?? ''}${stderr ?? ''}`.trim(),
      })
    })
  })
}

/** 原子追加一行（tmp+mv+回读）。 */
async function appendRow(path, row) {
  let text = ''
  try {
    text = await readFile(path, 'utf8')
  } catch {
    text = ''
  }
  if (text !== '' && !text.endsWith('\n')) text += '\n'
  const next = text + JSON.stringify(row) + '\n'
  const tmp = `${path}.tmp`
  await writeFile(tmp, next, 'utf8')
  await rename(tmp, path)
  const back = await readFile(path, 'utf8')
  if (back !== next) throw new Error(`回读不一致：${path}`)
  return next.split('\n').filter(line => line.trim() !== '').length
}

/** 本地时戳（Asia/Shanghai）：ISO 无时区会被误读成 UTC，台账统一带 +08:00。 */
function stamp() {
  const local = new Date().toLocaleString('sv-SE', { timeZone: 'Asia/Shanghai', hour12: false })
  return `${local.replace(' ', 'T')}+08:00`
}

function clip(text, limit) {
  const flat = String(text ?? '').replace(/\s+/g, ' ').trim()
  return flat.length > limit ? flat.slice(0, limit) + '…' : flat
}

/** 派发前的白名单：无 shell、script 必在 scripts/ 下。 */
function armable(rule) {
  if (rule['启用'] === false) return { ok: false, why: '启用:false' }
  const argv = rule['argv']
  if (!Array.isArray(argv) || argv.length < 2 || argv.some(item => typeof item !== 'string')) {
    return { ok: false, why: '未给 argv 数组（旧命令串不自动派发）' }
  }
  if (argv[0] !== 'python3') return { ok: false, why: 'argv[0] 须为 python3' }
  if (!argv[1].startsWith('scripts/')) return { ok: false, why: 'argv[1] 须落在 scripts/ 下' }
  return { ok: true, argv }
}

async function loadRules(root) {
  try {
    const text = await readFile(join(root, 'scripts', '事件触发规则.json'), 'utf8')
    const parsed = JSON.parse(text)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

/** 快照：追加=行数；新文件=文件名集合；mtime=时间戳。 */
async function snapshot(root, source, mode) {
  const path = join(root, source)
  try {
    if (mode === '追加') {
      const text = await readFile(path, 'utf8')
      return text.split('\n').filter(line => line.trim() !== '').length
    }
    if (mode === '新文件') {
      const names = (await readdir(path)).filter(name => !name.endsWith('.tmp'))
      return names.sort()
    }
    return (await stat(path)).mtimeMs
  } catch {
    return mode === '新文件' ? [] : 0
  }
}

function changed(mode, prev, now) {
  if (mode === '新文件') {
    const before = new Set(prev ?? [])
    return now.some(name => !before.has(name))
  }
  return Number(now) > Number(prev ?? 0)
}

/** 两态裁决：判"此事该事件触发还是令文触发"（启发式·判据一刀见 AGENTS 事件触发总线令）。 */
const EVENT_HINTS = ['定时', '每天', '每周', '自动', '落盘后', '新增', '变化', '失败', '后台', '常驻', '无人值守', '触发', '轮询', '每次']
const ORDER_HINTS = ['我说', '我发话', '对话', '问我', '答复', '汇报', '判断', '口吻', '语气', '写完', '这次']

function twoStateVerdict(text) {
  const event = EVENT_HINTS.filter(word => text.includes(word))
  const order = ORDER_HINTS.filter(word => text.includes(word))
  const verdict = event.length > order.length ? '事件触发' : (order.length > 0 ? '令文触发' : '判不出（缺语境）')
  const lines = [
    `裁决：${verdict}`,
    `事件线索 ${event.length}：${event.join('、') || '无'}`,
    `令文线索 ${order.length}：${order.join('、') || '无'}`,
    '',
    '判据一刀（AGENTS 事件触发总线令）：主人不发话，此事该不该在对话之外自己发生？',
    '该＝事件总线（挂规则，不挂 cron）；不该＝写进令文（第一梯队令，靠提示在场）。',
  ]
  if (verdict === '判不出（缺语境）') lines.push('补一句：触发源是什么？谁不发话也该跑？')
  return lines.join('\n')
}

export function apply(ctx, config) {
  const cfg = normalizeConfig(config)
  if (!cfg.enabled) {
    ctx.logger?.info?.('ql-bus：主开关关着，未挂载')
    return
  }

  const root = findRoot(cfg.root)
  const snapshots = new Map()
  const state = { lastTick: '', lastDispatch: '', ticks: 0, armed: 0, skipped: 0 }

  const dispatch = async (rule, argv) => {
    const started = stamp()
    const result = await runFile(argv[0], argv.slice(1), root, 120000)
    const tail = clip(result.out, 160)
    await appendRow(join(root, 'scripts', '事件触发台账.jsonl'), {
      时: started,
      事件: `${rule['源']}:${rule['模式']}`,
      命令: argv.join(' '),
      结果: `exit${result.code}:${tail}`,
    }).catch(() => undefined)
    state.lastDispatch = `${started}｜${rule['源']}:${rule['模式']}｜exit${result.code}`
    if (result.code !== 0) {
      const failures = join(root, 'scripts', '失败事件.jsonl')
      let recent = ''
      try {
        recent = await readFile(failures, 'utf8')
      } catch {
        recent = ''
      }
      const now = Date.now()
      const duplicated = recent.split('\n').filter(line => line.trim() !== '').some(line => {
        try {
          const row = JSON.parse(line)
          return row['源'] === rule['源'] && row['模式'] === rule['模式'] && now - (row['ts'] ?? 0) < 86400000
        } catch {
          return false
        }
      })
      if (!duplicated) {
        await appendRow(failures, {
          时: started,
          ts: now,
          源: rule['源'],
          模式: rule['模式'],
          命令: argv.join(' '),
          结果: `exit${result.code}:${tail}`,
          处理: 'issue log自动立案+次日重试位',
        }).catch(() => undefined)
        await appendRow(join(root, 'scripts', 'issue log.jsonl'), {
          时间: started,
          类型: '自动执行失败',
          源: 'ql-bus',
          错误: `exit${result.code}:${tail}`.slice(0, 120),
          案由: `事件派发失败（${rule['源']}:${rule['模式']}）`,
        }).catch(() => undefined)
      }
    }
    return { code: result.code, out: tail }
  }

  const tick = async () => {
    state.ticks += 1
    state.lastTick = stamp()
    const rules = await loadRules(root)
    state.armed = rules.filter(rule => armable(rule).ok).length
    state.skipped = rules.length - state.armed
    if (!cfg.autoDispatch) return
    for (const rule of rules) {
      const armed = armable(rule)
      if (!armed.ok) continue
      const key = `${rule['源']}|${rule['模式']}`
      const now = await snapshot(root, rule['源'], rule['模式'])
      const prev = snapshots.get(key)
      snapshots.set(key, now)
      if (prev === undefined) continue // 第一轮建基线
      if (changed(rule['模式'], prev, now)) await dispatch(rule, armed.argv)
    }
  }

  // 定时器归 Cordis 管：卸载时清掉，不留野循环。
  ctx.effect(() => {
    const handle = setInterval(() => { void tick().catch(() => undefined) }, cfg.pollSeconds * 1000)
    void tick().catch(() => undefined) // 首个基线立即建，不等第一个周期
    return () => clearInterval(handle)
  }, 'ql-bus.interval()')

  ctx.effect(() => ctx.tools.register({
    name: 'ql_bus',
    description: '事件总线：规则清单（哪些纳管/哪些未纳管及原因）、台账末五条、探活（轮询周期/上次心跳/上次派发/纳管数）、'
      + '试派发（按源名手动跑一次白名单规则）。不挂 cron，派发无 shell。',
    parameters: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['规则清单', '台账', '探活', '试派发', '两态裁决'],
          description: '要执行的动作。',
        },
        query: { type: 'string', description: '试派发＝规则源名（如 scripts/槽位采样.jsonl）。' },
      },
      required: ['action'],
      additionalProperties: false,
    },
    output: {
      schema: {
        type: 'object',
        properties: { report: { type: 'string' } },
        required: ['report'],
        additionalProperties: false,
      },
      render: (_args, value) => [{ type: 'text', text: value.report }],
    },
    execute: async (args) => {
      const action = typeof args?.action === 'string' ? args.action : '探活'
      const query = typeof args?.query === 'string' ? args.query.trim() : ''
      try {
        if (action === '探活') {
          // 纳管数现算，不等人事定时器跑过（否则刚挂载时读数是 0）。
          const live = await loadRules(root)
          const armedNow = live.filter(rule => armable(rule).ok).length
          return {
            report: [
              '【事件总线·探活】',
              `根目录：${root}`,
              `轮询：每 ${cfg.pollSeconds} 秒｜自动派发：${cfg.autoDispatch ? '开' : '关'}｜心跳次数 ${state.ticks}`,
              `上次心跳：${state.lastTick || '尚未'}｜上次派发：${state.lastDispatch || '尚未'}`,
              `纳管规则 ${armedNow} 条｜未纳管 ${live.length - armedNow} 条`,
            ].join('\n'),
          }
        }
        if (action === '两态裁决') {
          if (query === '') return { report: '两态裁决需要 query（描述这件事）。' }
          return { report: twoStateVerdict(query) }
        }
        if (action === '规则清单') {
          const rules = await loadRules(root)
          if (rules.length === 0) return { report: '【规则清单】无规则（scripts/事件触发规则.json 缺或空）。' }
          const lines = [`【规则清单】共 ${rules.length} 条：`]
          for (const rule of rules) {
            const armed = armable(rule)
            lines.push(`  · ${rule['源']}｜${rule['模式']}｜${armed.ok ? '✅纳管' : '⛔未纳管：' + armed.why}`)
            if (armed.ok) lines.push(`      派发：${armed.argv.join(' ')}`)
          }
          return { report: lines.join('\n') }
        }
        if (action === '台账') {
          let rows = []
          try {
            rows = (await readFile(join(root, 'scripts', '事件触发台账.jsonl'), 'utf8'))
              .split('\n').filter(line => line.trim() !== '')
          } catch {
            rows = []
          }
          if (rows.length === 0) return { report: '【事件台账】无记录。' }
          const lines = [`【事件台账】共 ${rows.length} 条，末五条：`]
          for (const line of rows.slice(-5)) {
            let row
            try {
              row = JSON.parse(line)
            } catch {
              lines.push(`  · ${clip(line, 120)}`)
              continue
            }
            lines.push(`  · ${row['时'] ?? '?'}｜${clip(row['事件'], 26)}｜${clip(row['结果'], 80)}`)
          }
          return { report: lines.join('\n') }
        }
        if (action === '试派发') {
          if (query === '') return { report: '试派发需要 query（规则源名）。' }
          const rules = await loadRules(root)
          const rule = rules.find(item => item['源'] === query)
          if (rule === undefined) return { report: `试派发：找不到源为「${query}」的规则。` }
          const armed = armable(rule)
          if (!armed.ok) return { report: `试派发被拦：${armed.why}` }
          const result = await dispatch(rule, armed.argv)
          return { report: `已试派发：${armed.argv.join(' ')} → exit${result.code}\n输出：${clip(result.out, 400)}` }
        }
        return { report: `未知动作：${action}` }
      } catch (error) {
        return { report: `总线内部错：${clip(error?.message ?? error, 200)}` }
      }
    },
  }), 'ql-bus.tool()')

  ctx.logger?.info?.('ql-bus：已挂载（每 %d 秒轮询·自动派发=%s）', cfg.pollSeconds, cfg.autoDispatch)
}

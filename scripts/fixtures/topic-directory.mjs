import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdir } from 'node:fs/promises'
import { DatabaseSync } from 'node:sqlite'
import { tmpdir } from 'node:os'
import { dirname, resolve, sep } from 'node:path'
import { StrataGate, memoryTopicEventFingerprint, MEMORY_TOPIC_PROJECTOR_VERSION } from '../../packages/core/dist/index.js'
import { SqliteStorage } from '../../packages/core/dist/sqlite.js'

export const topicBrowserFixtures = {
  normal: 'browser:topic-directory:normal',
  pending: 'browser:topic-directory:pending',
  running: 'browser:topic-directory:running',
  failed: 'browser:topic-directory:failed',
  empty: 'browser:topic-directory:empty',
  ordered: 'browser:topic-directory:ordered',
  topicIds: ['topic_browser_a', 'topic_browser_b'],
  laterTopicTitle: '后来加入的主题',
  raw: '浏览器验收原始消息：Event 是历史事实的权威来源，Graph 表达当前状态。',
}

export function assertDisposableDatabase(database) {
  if (!database || !resolve(database).toLowerCase().startsWith((resolve(tmpdir()) + sep).toLowerCase())) {
    throw new Error('Topic browser regression may access only a disposable database under the system temp directory')
  }
}

export async function seedTopicBrowserFixtures(database) {
  assertDisposableDatabase(database)
  await mkdir(dirname(resolve(database)), { recursive: true })
  const memory = StrataGate.inMemory({
    blockTurnSize: 1,
    disableElementProjection: true,
    summarizer: async () => ({ l0Title: '目录验收原文', l0Tags: ['记忆'], l1Summary: '目录验收原文', l2Keypoints: [], shouldExtract: false }),
  })
  const storage = new SqliteStorage({ filename: database })
  try {
    if (storage.listNamespaces().some((namespace) => namespace.startsWith('browser:topic-directory:'))) {
      throw new Error('Topic fixtures require a fresh disposable database. Use --no-seed for a profile that is already seeded.')
    }
    await memory.appendTurn({ user: topicBrowserFixtures.raw, assistant: '已记录，可以从事件追溯这条原文。' })
    const block = memory.listBlocks()[0]
    for (let index = 0; index < 25; index++) {
      await memory.addEvent({
        title: `记忆机制演进 ${String(index + 1).padStart(2, '0')}：保留原始依据与稳定主题入口`,
        summary: `第 ${index + 1} 次设计确认：主题负责整理跨事件的脉络，查看页面不产生记忆采用。`,
        sourceBlockId: block.id,
        sourceMessageIds: [block.l5Raw[0].id],
        temporal: { status: 'occurred', eventType: 'decision' },
      })
    }
    for (let index = 0; index < 2; index++) {
      const job = await memory.claimNextTopicProjection()
      assert.equal(job?.events.length, 12)
      const ids = job.events.map(({ id }) => id)
      const sections = index === 0
        ? [
            { kind: 'history', text: '八条事件构成这一节的发展脉络。原始事实与来源始终保留。', sourceEventIds: ids.slice(0, 8) },
            { kind: 'decision', text: '九条关键设计决策应与总览一起构成默认十项，无需展开全部。', sourceEventIds: ids.slice(0, 9) },
            { kind: 'change', text: '十二条重要变化超过默认九条事件，按需展开后仍可收起。', sourceEventIds: ids },
            { kind: 'open-question', text: '一个尚未解决的问题，保留到对应事件和原始对话的入口。', sourceEventIds: ids.slice(0, 1) },
          ]
        : Array.from({ length: 8 }, (_, section) => ({
            kind: ['history', 'decision', 'change', 'open-question', 'scope'][section % 5],
            text: `第 ${section + 1} 节的总览：${'这是较长的中文脉络说明，检验窄窗口换行与多节同时展开后的滚动体验。'.repeat(3)}`,
            sourceEventIds: ids.slice(0, section % 3 === 0 ? 1 : section % 3 === 1 ? 9 : 12),
          }))
      await memory.completeTopicProjection(job.jobId, { topics: [{
        title: index === 0 ? 'StrataGate 记忆机制' : '求职与实习方向的长期规划，以及不同团队机会、个人偏好与记忆研究方向之间的持续比较和阶段性选择'.repeat(2),
        description: index === 0 ? '架构演进、关键决策、历史变化与待确认的问题' : '较长标题、多节目录与原始事件的浏览验收',
        sourceEventIds: ids,
        overview: sections,
      }] })
    }
    const snapshot = memory.exportSnapshot()
    const oldIds = snapshot.memoryTopicState.topics.map(({ id }) => id)
    snapshot.memoryTopicState.topics.forEach((topic, index) => {
      topic.id = topicBrowserFixtures.topicIds[index]
      topic.createdAt = `2026-10-01T0${index + 1}:00:00.000Z`
    })
    for (const job of snapshot.memoryTopicState.jobs) job.topicIds = job.topicIds.map((id) => topicBrowserFixtures.topicIds[oldIds.indexOf(id)])
    const now = new Date().toISOString()
    const versions = Object.fromEntries(snapshot.events.map((event) => [event.id, memoryTopicEventFingerprint(event)]))
    const fallback = snapshot.events.find(({ id }) => !(id in snapshot.memoryTopicState.projectedVersions))
    assert.ok(fallback)
    for (const variant of ['normal', 'pending', 'running', 'failed', 'ordered']) {
      const next = structuredClone(snapshot)
      if (variant === 'normal' || variant === 'ordered') {
        next.memoryTopicState.bootstrap = {
          ...next.memoryTopicState.bootstrap,
          sourceVersions: { ...next.memoryTopicState.projectedVersions },
        }
      }
      if (variant === 'ordered') {
        next.memoryTopicState.topics.forEach((topic, index) => {
          topic.title = index === 0 ? '原有主题甲' : '原有主题乙'
          topic.description = '按形成时间显示的主题'
        })
        next.memoryTopicState.topics.push({
          id: `topic_000_${randomUUID()}`,
          title: topicBrowserFixtures.laterTopicTitle,
          description: '这是后来形成的新章，原有两章的顺序和编号应该保持。',
          sourceEventIds: [fallback.id],
          sourceVersions: { [fallback.id]: versions[fallback.id] },
          dependencyVersions: { [fallback.id]: versions[fallback.id] },
          projectorVersion: MEMORY_TOPIC_PROJECTOR_VERSION,
          invalidated: false,
          overview: [{ kind: 'scope', text: '新增主题只追加在已有主题之后。', sourceEventIds: [fallback.id] }],
          createdAt: '2026-10-01T03:00:00.000Z',
          updatedAt: now,
        })
        next.memoryTopicState.projectedVersions[fallback.id] = versions[fallback.id]
      }
      if (!['normal', 'ordered'].includes(variant)) {
        next.memoryTopicState.bootstrap = {
          projectorVersion: MEMORY_TOPIC_PROJECTOR_VERSION,
          sourceVersions: versions,
          status: variant === 'failed' ? 'completed' : variant,
          startedAt: now,
          completedAt: variant === 'failed' ? now : null,
          failedEvents: variant === 'failed' ? 1 : 0,
        }
        if (variant !== 'pending') next.memoryTopicState.jobs.push({
          id: `topic_browser_${variant}_job`,
          sourceEventIds: [fallback.id],
          sourceVersions: { [fallback.id]: versions[fallback.id] },
          dependencyVersions: { [fallback.id]: versions[fallback.id] },
          candidateVersions: {},
          context: null,
          projectorVersion: MEMORY_TOPIC_PROJECTOR_VERSION,
          status: variant === 'failed' ? 'failed' : 'running',
          attempts: variant === 'failed' ? 3 : 1,
          topicIds: [],
          lastError: variant === 'failed' ? 'validation-failed: every supplied batch Event must be assigned to a topic' : null,
          ...(variant === 'failed' ? { diagnostics: { category: 'validation-failed',
            reason: 'every supplied batch Event must be assigned to a topic', eventCount: 1, candidateTopicCount: 2,
            estimatedInputTokens: 3000, requestedOutputTokens: 32768, maxOutputTokens: 32768, finishReason: 'stop',
            reasoningOff: 'unavailable', modelCalls: 2, attempt: 3, splitDepth: 0 } } : {}),
          nextRetryAt: null,
          createdAt: now,
          updatedAt: now,
          leaseUntil: variant === 'running' ? new Date(Date.now() + 60 * 60_000).toISOString() : null,
        })
      }
      const namespace = topicBrowserFixtures[variant]
      const loaded = await storage.load(namespace)
      await storage.save(namespace, next, loaded?.revision ?? 0)
    }
    const empty = StrataGate.inMemory({ disableElementProjection: true }).exportSnapshot()
    const loaded = await storage.load(topicBrowserFixtures.empty)
    await storage.save(topicBrowserFixtures.empty, empty, loaded?.revision ?? 0)
    return { namespaces: ['normal', 'pending', 'running', 'failed', 'empty', 'ordered'].map((key) => topicBrowserFixtures[key]), events: 25, topics: 2, orderedTopics: 3 }
  } finally {
    await storage.close()
    await memory.close()
  }
}

// Compare durable rows, including receipts, weights, model-response records and
// integration metadata. Readonly connections cannot create a topic state table.
export function durableBrowserSnapshot(database) {
  assertDisposableDatabase(database)
  const db = new DatabaseSync(database, { readOnly: true })
  try {
    return Object.fromEntries(db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map(({ name }) => {
      const quoted = `"${name.replaceAll('"', '""')}"`
      const rows = db.prepare(`SELECT * FROM ${quoted}`).all().map((row) => JSON.stringify(row)).sort()
      return [name, rows]
    }))
  } finally {
    db.close()
  }
}

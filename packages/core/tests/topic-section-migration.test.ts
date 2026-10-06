import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { StrataGate, MemoryTopicDirectory, MEMORY_TOPIC_PROJECTOR_VERSION, type TopicProjectionContext } from '../src/index.js';
import { SqliteStorage } from '../src/sqlite.js';

const options = { blockTurnSize: 1, disableElementProjection: true,
  summarizer: async () => ({ l0Title: '原文', l0Tags: [], l1Summary: '项目原文', l2Keypoints: [], shouldExtract: false }) };
function projection(context: TopicProjectionContext, section = '缺陷排查与修复') {
  return { topics: [{ ...(context.existingTopics[0] ? { topicId: context.existingTopics[0].id } : {}),
    title: 'StrataGate', description: '项目开发与排查记录', sourceEventIds: context.events.map(({ id }) => id),
    overview: [{ kind: 'history' as const, title: section, text: '历史排查记录 ' + context.events[0]!.id,
      sourceEventIds: context.events.map(({ id }) => id) }] }] };
}
async function seed(memory: StrataGate, count: number) {
  await memory.appendTurn({ user: '原始缺陷排查过程', assistant: '已记录' });
  const block = memory.listBlocks()[0]!;
  for (let index = 0; index < count; index++) await memory.addEvent({ id: 'bug-' + index,
    title: '0.2.' + index + ' 提取排查', summary: '第 ' + index + ' 项缺陷仍待确认',
    sourceBlockId: block.id, sourceMessageIds: [block.l5Raw[0]!.id] });
}
function downgrade(state: ReturnType<StrataGate['exportSnapshot']>['memoryTopicState']) {
  state!.bootstrap!.projectorVersion = 2;
  for (const topic of state!.topics) topic.projectorVersion = 2;
  for (const job of state!.jobs) job.projectorVersion = 2;
}

describe('lasting section categories and V2 chapter preservation', () => {
  it('regroups persisted V2 sections once, retaining chapter identity, facts and restart progress', async () => {
    const root = await mkdtemp(join(tmpdir(), 'stratagate-section-migration-'));
    const database = join(root, 'memory.db'), namespace = 'project:section-rules';
    let memory: StrataGate | undefined;
    try {
      memory = await StrataGate.open({ ...options, database, namespace });
      await seed(memory, 29);
      while (memory.hasPendingTopicWork()) {
        const job = (await memory.claimNextTopicProjection())!;
        const result = projection(job);
        result.topics[0]!.overview = job.events.filter((_, index) => index % 2 === 0).map((event, index) => ({
          kind: 'history' as const, title: event.title, text: event.summary,
          sourceEventIds: job.events.slice(index * 2, index * 2 + 2).map(({ id }) => id),
        }));
        await memory.completeTopicProjection(job.jobId, result);
      }
      const baseline = memory.exportSnapshot();
      const oldChapter = baseline.memoryTopicState!.topics[0]!;
      expect(oldChapter.overview.length).toBeGreaterThan(8);
      const lateJobId = baseline.memoryTopicState!.jobs[0]!.id;
      await memory.close(); memory = undefined;
      const writer = new SqliteStorage({ filename: database });
      try {
        const loaded = (await writer.load(namespace))!; downgrade(loaded.snapshot.memoryTopicState);
        await writer.save(namespace, loaded.snapshot, loaded.revision);
      } finally { await writer.close(); }
      const readStorage = new SqliteStorage({ filename: database, readonly: true });
      const beforeRead = (await readStorage.load(namespace))!;
      const reader = await StrataGate.openWithStorage({ ...options, storage: readStorage, namespace });
      try {
        expect(reader.listMemoryTopics()[0]).toMatchObject({ id: oldChapter.id, title: 'StrataGate', overview: [] });
        expect(reader.getTopicBootstrapState()).toBeNull();
        expect(await readStorage.load(namespace)).toEqual(beforeRead);
      } finally { await reader.close(); }
      memory = await StrataGate.open({ ...options, database, namespace });
      const migrated = memory.exportSnapshot().memoryTopicState!;
      expect(migrated.topics).toHaveLength(1);
      expect(migrated.topics[0]).toMatchObject({ id: oldChapter.id, title: oldChapter.title,
        description: oldChapter.description, createdAt: oldChapter.createdAt, sourceEventIds: oldChapter.sourceEventIds,
        overview: [], projectorVersion: MEMORY_TOPIC_PROJECTOR_VERSION, invalidated: false });
      expect(migrated.jobs).toEqual([]); expect(migrated.projectedVersions).toEqual({});
      expect(memory.hasPendingTopicWork('incremental')).toBe(false);
      expect(Object.keys(memory.getTopicBootstrapState()!.sourceVersions)).toHaveLength(29);
      const first = (await memory.claimNextTopicProjection('bootstrap'))!;
      expect(first.existingTopics[0]).toMatchObject({ id: oldChapter.id, title: 'StrataGate', overview: [], sectionTitles: [] });
      await expect(memory.completeTopicProjection(lateJobId, projection(first))).rejects.toThrow(/Unknown topic projection/);
      expect((await memory.completeTopicProjection(first.jobId, projection(first))).topicIds).toEqual([oldChapter.id]);
      const progress = memory.exportSnapshot().memoryTopicState!;
      const revision = memory.storageRevision;
      await memory.close(); memory = await StrataGate.open({ ...options, database, namespace });
      expect(memory.storageRevision).toBe(revision);
      expect(memory.exportSnapshot().memoryTopicState).toEqual(progress);
      const block = memory.listBlocks()[0]!;
      await memory.addEvent({ id: 'new-bug', title: '新的缺陷', summary: '新的排查记录', sourceBlockId: block.id,
        sourceMessageIds: [block.l5Raw[0]!.id] });
      const fresh = (await memory.claimNextTopicProjection('incremental'))!;
      expect(fresh.events.map(({ id }) => id)).toEqual(['new-bug']);
      await memory.completeTopicProjection(fresh.jobId, projection(fresh));
      while (memory.hasPendingTopicWork('bootstrap')) {
        const job = (await memory.claimNextTopicProjection('bootstrap'))!;
        expect(job.existingTopics[0]!.sectionTitles).toEqual(['缺陷排查与修复']);
        expect((await memory.completeTopicProjection(job.jobId, projection(job))).topicIds).toEqual([oldChapter.id]);
      }
      const after = memory.exportSnapshot();
      expect(after.memoryTopicState!.topics[0]!.sourceEventIds).toEqual([...oldChapter.sourceEventIds, 'new-bug']);
      expect(new Set(after.memoryTopicState!.topics[0]!.overview.map(({ title }) => title))).toEqual(new Set(['缺陷排查与修复']));
      expect(new Set(after.memoryTopicState!.topics[0]!.overview.flatMap(({ sourceEventIds }) => sourceEventIds)).size).toBe(30);
      for (const field of ['agentEvents', 'blocks', 'openTail', 'graphNodes', 'graphEdges', 'usageReceipts'] as const) expect(after[field]).toEqual(baseline[field]);
      expect(after.events.filter(({ id }) => id !== 'new-bug')).toEqual(baseline.events);
      await memory.close(); memory = await StrataGate.open({ ...options, database, namespace });
      expect(memory.hasPendingTopicWork()).toBe(false);
      expect(memory.listMemoryTopics()[0]!.id).toBe(oldChapter.id);
    } finally { await memory?.close(); await rm(root, { recursive: true, force: true }); }
  });

  it('does not retain chapter language backed by hidden members or changed uncited dependencies', async () => {
    const memory = StrataGate.inMemory(options); await seed(memory, 3);
    const events = memory.listAllEvents();
    const job = (await memory.claimNextTopicProjection())!;
    await memory.completeTopicProjection(job.jobId, { topics: job.events.map((event) => ({ title: event.title,
      description: event.summary, sourceEventIds: [event.id], overview: [] })) });
    const old = memory.exportSnapshot().memoryTopicState!; downgrade(old);
    // Every chapter read all three Events as background, even if it cited one.
    events[2]!.status = 'forgotten';
    const directory = new MemoryTopicDirectory(); directory.restore(old);
    directory.initializeBootstrap(events, new Date().toISOString());
    expect(directory.snapshot().topics).toEqual([]);
    expect(Object.keys(directory.bootstrap()!.sourceVersions)).toHaveLength(2);
    expect(directory.claim(events, new Date().toISOString(), 'incremental')).toBeNull();
    await memory.close();
  });

  it('keeps multiple V2 chapter positions and rejects mixed generations instead of trusting old prose', async () => {
    const memory = StrataGate.inMemory(options); await seed(memory, 3);
    const events = memory.listAllEvents(); const job = (await memory.claimNextTopicProjection())!;
    await memory.completeTopicProjection(job.jobId, { topics: job.events.map((event) => ({ title: event.title,
      description: event.summary, sourceEventIds: [event.id], overview: [] })) });
    const old = memory.exportSnapshot().memoryTopicState!; downgrade(old);
    const directory = new MemoryTopicDirectory(); directory.restore(old);
    directory.initializeBootstrap(events, new Date().toISOString());
    expect(directory.snapshot().topics.map(({ id, title, createdAt, sourceEventIds }) => ({ id, title, createdAt, sourceEventIds })))
      .toEqual(old.topics.map(({ id, title, createdAt, sourceEventIds }) => ({ id, title, createdAt, sourceEventIds })));
    old.jobs[0]!.projectorVersion = 1;
    const mixed = new MemoryTopicDirectory(); mixed.restore(old); mixed.initializeBootstrap(events, new Date().toISOString());
    expect(mixed.snapshot().topics).toEqual([]);
    await memory.close();
  });
});

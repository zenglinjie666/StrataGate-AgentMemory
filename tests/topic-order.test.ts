import { estimateTokens, type EventCard, type MemoryTopic } from '@diqier/stratagate'
import { describe, expect, it } from 'vitest'
import { boundedTopic, MEMORY_DIRECTORY_TOKEN_BUDGET, TOPIC_OVERVIEW_TOKEN_BUDGET, renderMemoryDirectory, sortMemoryTopics, topicNavigation, topicPage } from '../src/topics.js'

function topic(id: string, createdAt: string, sourceEventIds = [id], isFallback = false): MemoryTopic {
  return { id, title: `主题 ${id}`, description: '', sourceEventIds, overview: [], createdAt,
    updatedAt: createdAt, coverage: { totalEvents: 1, summarizedEvents: 1, omittedEvents: 0 },
    ...(isFallback ? { isFallback: true } : {}) }
}

function source(id: string, eventType: string): EventCard {
  return { id, title: id, summary: '', tags: [], quotes: [], sourceMessageIds: [], sourceBlockId: 'block',
    temporal: { eventType }, scope: 'project', criticality: 'routine', status: 'active', supersededBy: null,
    weight: { mentionCount: 1, lastAdoptedTurn: 0, lastRetrievedAt: null, pinned: false, floorWeight: 0, forcedCap: null },
    createdAt: '2026-10-01T00:00:00.000Z', updatedAt: '2026-10-01T00:00:00.000Z' }
}

describe('shared memory directory order', () => {
  it('keeps the full independent membership index outside the bounded model expansion', () => {
    const chapter = topic('large', '2026-10-01T00:00:00.000Z', Array.from({ length: 10_000 }, (_, index) => `member-${index}`));
    chapter.sections = [{ title: '研究方向', sourceEventIds: [...chapter.sourceEventIds] }];
    const before = JSON.stringify(chapter); const expanded = boundedTopic(chapter);
    expect(expanded.sections).toEqual([{ title: '研究方向', sourceEventCount: 10_000 }]);
    expect(expanded.totalSections).toBe(1);
    expect(expanded.omittedSections).toBe(0);
    expect(expanded.sourceEventIds).toHaveLength(12);
    expect(expanded.omittedSourceEvents).toBe(9_988);
    expect(estimateTokens(JSON.stringify(expanded))).toBeLessThanOrEqual(TOPIC_OVERVIEW_TOKEN_BUDGET);
    expect(JSON.stringify(chapter)).toBe(before);
  });

  it('bounds a large lightweight section index and reports every omitted section without leaking member ids', () => {
    const chapter = topic('large-index', '2026-10-01T00:00:00.000Z', ['member']);
    chapter.sections = Array.from({ length: 300 }, (_, index) => ({ title: `类别 ${index} ${'长期类别'.repeat(15)}`, sourceEventIds: ['member'] }));
    chapter.overview = Array.from({ length: 8 }, (_, index) => ({ kind: 'scope' as const, title: chapter.sections![index]!.title,
      text: '资料范围'.repeat(100), sourceEventIds: ['member'] }));
    const envelope = { namespace: 'dsh:project:budget', navigationOnly: true, note: '这是导航，需要继续查证事件。' };
    const expanded = boundedTopic(chapter, envelope);
    expect(expanded.sections.length).toBeGreaterThan(0);
    expect(expanded.omittedSections).toBeGreaterThan(0);
    expect(expanded.sections.length + expanded.omittedSections).toBe(300);
    expect(expanded.totalSections).toBe(300);
    expect(expanded.overview.length + expanded.omittedOverviewParagraphs).toBe(8);
    expect(expanded.sections.every((section) => Object.keys(section).sort().join(',') === 'sourceEventCount,title')).toBe(true);
    expect(expanded.sections.every((section) => section.sourceEventCount === 1)).toBe(true);
    expect(estimateTokens(JSON.stringify({ ...envelope, topic: expanded }))).toBeLessThanOrEqual(TOPIC_OVERVIEW_TOKEN_BUDGET);
  });

  it('keeps known legacy section labels in the Agent navigation index', () => {
    const chapter = topic('legacy', '2026-10-01T00:00:00.000Z', ['E1', 'E2']);
    chapter.overview = [{ kind: 'scope', title: '研究方向', text: '资料范围', sourceEventIds: ['E1', 'E2'] }];
    expect(boundedTopic(chapter).sections).toEqual([{ title: '研究方向', sourceEventCount: 2 }]);
  });

  it('keeps same-batch chapters in immutable ID order when their titles change', () => {
    const first = topic('topic_a', '2026-10-01T00:00:00.000Z')
    const second = topic('topic_b', first.createdAt)
    first.title = '甲主题'
    second.title = '乙主题'
    const topics = [second, first]
    const expected = ['topic_a', 'topic_b']
    expect(sortMemoryTopics(topics).map(({ id }) => id)).toEqual(expected)
    first.title = 'Z renamed'
    second.title = 'A renamed'
    first.updatedAt = '2026-10-04T00:00:00.000Z'
    expect(sortMemoryTopics(topics).map(({ id }) => id)).toEqual(expected)
    expect(topicNavigation(topics, []).entries.map(({ id }) => id)).toEqual(expected)
    expect(topicPage(topics, []).topics.map(({ id }) => id)).toEqual(expected)
    const context = renderMemoryDirectory(topics, [])
    expect(context.indexOf('- topic_a：')).toBeLessThan(context.indexOf('- topic_b：'))
  })

  it('appends newly created topics even when their generated id sorts before existing chapters', () => {
    const first = topic('topic_z', '2026-10-01T00:00:00.000Z')
    const second = topic('topic_b', '2026-10-02T00:00:00.000Z')
    const newest = topic('topic_000', '2026-10-03T00:00:00.000Z')
    const input = [newest, second, first]
    expect(sortMemoryTopics(input).map(({ id }) => id)).toEqual([first.id, second.id, newest.id])
    expect(input.map(({ id }) => id)).toEqual([newest.id, second.id, first.id])
    first.title = '改名后的早期主题'
    expect(sortMemoryTopics(input).map(({ id }) => id)).toEqual([first.id, second.id, newest.id])
  })

  it('uses the same creation order in navigation, pagination and injected context across categories', () => {
    const oldest = topic('topic_z', '2026-10-01T00:00:00.000Z')
    const newest = topic('topic_a', '2026-10-03T00:00:00.000Z')
    const topics = [newest, oldest]
    const events = [source(oldest.id, 'release'), source(newest.id, 'decision')]
    const expected = [oldest.id, newest.id]
    expect(topicNavigation(topics, events).entries.map(({ id }) => id)).toEqual(expected)
    expect(topicPage(topics, events).topics.map(({ id }) => id)).toEqual(expected)
    const context = renderMemoryDirectory(topics, events)
    expect(context.indexOf(`- ${oldest.id}：`)).toBeLessThan(context.indexOf(`- ${newest.id}：`))
  })

  it('keeps fallback entries after formal chapters without changing their stable source ids', () => {
    const fallback = topic('fallback:old-event', '2026-09-01T00:00:00.000Z', ['old-event'], true)
    const formal = topic('topic_z', '2026-10-01T00:00:00.000Z')
    expect(sortMemoryTopics([fallback, formal])).toEqual([formal, fallback])
    expect(fallback.sourceEventIds).toEqual(['old-event'])
  })

  it('orders large-catalog representatives consistently while preserving the existing context budget', () => {
    const topics = Array.from({ length: 60 }, (_, index) => topic(`topic_${index}`, new Date(Date.UTC(2026, 8, index + 1)).toISOString()))
    const events = topics.map(({ id }, index) => source(id, index % 2 === 0 ? 'release' : 'decision'))
    const context = renderMemoryDirectory([...topics].reverse(), events)
    const ids = [...context.matchAll(/^- (topic_\d+)：/gm)].map((match) => match[1])
    expect(ids.length).toBeGreaterThan(0)
    expect(ids).toEqual(ids.slice().sort((a, b) => Number(a!.slice(6)) - Number(b!.slice(6))))
    expect(estimateTokens(context)).toBeLessThanOrEqual(MEMORY_DIRECTORY_TOKEN_BUDGET)
    expect(context).toContain('共 60 项')
  })
})

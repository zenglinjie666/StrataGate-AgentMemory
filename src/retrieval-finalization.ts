import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import type { Session } from '@deepseek-ai/dsh-session'
import { dshMessageSource } from './dsh-compatibility.js'
import type { StrataGateRuntime } from './runtime.js'

interface Recovery {
  phase: 'closing' | 'answer'
  pending: Set<string>
  remainingStops: number
}

const FINAL_ANSWER_INSTRUCTION = 'Resume the original real user request in this turn, not the plugin instructions. All omitted memory receipts have now been handled. Now produce the complete final user-facing answer as the last assistant text. If you already gave the answer before the receipts, restate it in full, preserving its supported facts, conclusions, and requested deliverables; do not merely refer to the earlier answer. Use only evidence actually adopted for that answer. Do not perform new retrieval or other tool work. Do not report batch closure, evidence-use bookkeeping, unresolvedBatchIds, or other StrataGate internal status; these are not the user\'s answer.'

/** Recover within the existing turn; never synthesize assistant log events. */
export function installRetrievalFinalization(ctx: Context, runtime: StrataGateRuntime): void {
  const recoveries = new WeakMap<Session, Recovery>()
  const steer = (agent: Agent, text: string) => agent.steer(createUserMessage({
    content: [{ type: 'text', text }],
    source: dshMessageSource('instructions'),
  }))
  const answer = (agent: Agent, recovery: Recovery) => {
    recovery.phase = 'answer'
    steer(agent, FINAL_ANSWER_INSTRUCTION)
  }

  ctx.on('agent/turn-stopping', ({ agent, signal }) => {
    if (signal?.aborted) return
    const pending = runtime.pendingBatchIds(agent.session)
    const recovery = recoveries.get(agent.session)
    if (pending.length === 0) {
      if (recovery?.phase === 'closing') answer(agent, recovery)
      else recoveries.delete(agent.session)
      return
    }
    // A model that ignores cleanup must fail visibly rather than loop forever.
    if (recovery && (recovery.remainingStops === 0 || ![...recovery.pending].some((id) => !pending.includes(id)))) {
      throw new Error('StrataGate retrieval cleanup made no progress; unresolved batches remain. Retry the turn to complete memory_record_use.')
    }
    recoveries.set(agent.session, { phase: 'closing', pending: new Set(pending), remainingStops: recovery ? recovery.remainingStops - 1 : pending.length })
    steer(agent, `StrataGate retrieval batches are still unresolved: ${pending.join(', ')}. Before answering, close every one with memory_record_use using its batch_id and exactly the evidence_refs actually adopted for the user answer, or [] if none were used. Non-empty refs still require that batch's sufficient memory_assess; do not reinforce evidence merely because it was retrieved. This is only the evidence-processing phase. Do not write a final answer or a batch-status explanation yet. After all receipts succeed, you will receive an instruction to produce the complete user answer again.`)
  })

  ctx.on('system-prompt/assemble', async (_assembly, context, next) => {
    const assembled = await next()
    const recovery = context.agent && recoveries.get(context.agent.session)
    if (!recovery || !context.agent || context.signal?.aborted) return assembled
    // Assembly runs after the entire tool step settles, including parallel calls.
    // Do not transition on an individual tools/result: a later call may create a batch.
    if (runtime.needsRecordUse(context.agent.session)) return assembled
    recovery.phase = 'answer'
    // The answer step cannot create another batch. The following turn regains tools.
    return { ...assembled, tools: [], contexts: [...assembled.contexts, { name: 'stratagate:final-answer-recovery', text: FINAL_ANSWER_INSTRUCTION }] }
  })
  ctx.on('session/event', (session, event) => {
    if (event.type === 'turn/end') recoveries.delete(session)
  })
}

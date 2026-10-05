import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import type { Session, SessionEvent } from '@deepseek-ai/dsh-session'
import { dshMessageSource } from './dsh-compatibility.js'
import type { StrataGateRuntime } from './runtime.js'

interface Recovery {
  turn: number
  phase: 'closing' | 'awaiting-answer' | 'answer-complete'
  pending: Set<string>
  remainingStops: number
  answerAttempts: number
  answerStep?: number
}

const MAX_ANSWER_ATTEMPTS = 3

function isCompleteAnswer(data: Extract<SessionEvent, { type: 'assistant/message' }>['data']): boolean {
  if (data.interrupted || data.message.content.some((block) => block.type === 'tool-call')) return false
  if (!data.message.content.some((block) => block.type === 'text' && block.text.trim().length > 0)) return false
  // DSH 0.2.0 embeds the terminal finish chunk in the durable message stream.
  // Older hosts without an embedded stream can still confirm visible text.
  const stream = data.stream ?? []
  for (let index = stream.length - 1; index >= 0; index--) {
    const record = stream[index]!
    if (record.type === 'chunk' && record.chunk.type === 'finish') return record.chunk.reason.kind === 'stop'
  }
  return true
}

function answerFailure(): Error {
  return new Error(`StrataGate final answer recovery failed after ${MAX_ANSWER_ATTEMPTS} attempts: no complete non-empty user-visible answer was confirmed. Retry the turn.`)
}

const FINAL_ANSWER_INSTRUCTION = 'Resume the original real user request in this turn, not the plugin instructions. All omitted memory receipts have now been handled. Now produce the complete final user-facing answer as the last assistant text. If you already gave the answer before the receipts, restate it in full, preserving its supported facts, conclusions, and requested deliverables; do not merely refer to the earlier answer. Use only evidence actually adopted for that answer. Do not perform new retrieval or other tool work. Do not report batch closure, evidence-use bookkeeping, unresolvedBatchIds, or other StrataGate internal status; these are not the user\'s answer.'

/** Recover within the existing turn; never synthesize assistant log events. */
export function installRetrievalFinalization(ctx: Context, runtime: StrataGateRuntime): void {
  const recoveries = new WeakMap<Session, Recovery>()
  const steer = (agent: Agent, text: string) => agent.steer(createUserMessage({
    content: [{ type: 'text', text }],
    source: dshMessageSource('instructions'),
  }))
  ctx.on('agent/turn-stopping', ({ agent, turn, signal }) => {
    if (signal?.aborted) return
    const pending = runtime.pendingBatchIds(agent.session)
    const recovery = recoveries.get(agent.session)
    if (pending.length === 0) {
      if (!recovery) return
      // Another stop listener may still steer. Keep confirmation until turn/end
      // so a later admitted step must produce its own complete final answer.
      if (recovery.phase === 'answer-complete') return
      if (recovery.answerAttempts >= MAX_ANSWER_ATTEMPTS) throw answerFailure()
      steer(agent, FINAL_ANSWER_INSTRUCTION)
      return
    }
    if (recovery && recovery.phase !== 'closing') {
      throw new Error('StrataGate final answer recovery created a new retrieval batch; final answer cannot be confirmed.')
    }
    // A model that ignores cleanup must fail visibly rather than loop forever.
    if (recovery && (recovery.remainingStops === 0 || ![...recovery.pending].some((id) => !pending.includes(id)))) {
      throw new Error('StrataGate retrieval cleanup made no progress; unresolved batches remain. Retry the turn to complete memory_record_use.')
    }
    recoveries.set(agent.session, { turn, phase: 'closing', pending: new Set(pending), remainingStops: recovery ? recovery.remainingStops - 1 : pending.length, answerAttempts: 0 })
    steer(agent, `StrataGate retrieval batches are still unresolved: ${pending.join(', ')}. Before answering, close every one with memory_record_use using its batch_id and exactly the evidence_refs actually adopted for the user answer, or [] if none were used. Non-empty refs still require that batch's sufficient memory_assess; do not reinforce evidence merely because it was retrieved. This is only the evidence-processing phase. Do not write a final answer or a batch-status explanation yet. After all receipts succeed, you will receive an instruction to produce the complete user answer again.`)
  })

  ctx.on('system-prompt/assemble', async (_assembly, context, next) => {
    const assembled = await next()
    const recovery = context.agent && recoveries.get(context.agent.session)
    if (!recovery || !context.agent || context.signal?.aborted) return assembled
    // Assembly runs after the entire tool step settles, including parallel calls.
    // Do not transition on an individual tools/result: a later call may create a batch.
    if (runtime.needsRecordUse(context.agent.session)) {
      if (recovery.phase !== 'closing') throw new Error('StrataGate final answer recovery created a new retrieval batch; final answer cannot be confirmed.')
      return assembled
    }
    // Do not admit another request after an exhausted failed answer, including
    // tool-call output or other steering that bypassed the stop boundary.
    if (recovery.phase !== 'answer-complete' && recovery.answerAttempts >= MAX_ANSWER_ATTEMPTS) throw answerFailure()
    // The answer step cannot create another batch. The following turn regains tools.
    return { ...assembled, tools: [], contexts: [...assembled.contexts, { name: 'stratagate:final-answer-recovery', text: FINAL_ANSWER_INSTRUCTION }] }
  })
  ctx.on('session/event', (session, event) => {
    const recovery = recoveries.get(session)
    if (!recovery) return
    if (event.type === 'turn/end' && event.data.turn === recovery.turn) {
      recoveries.delete(session)
    } else if (event.type === 'step/start' && event.data.turn === recovery.turn) {
      if (recovery.phase === 'closing' && runtime.needsRecordUse(session)) return
      // Count admitted steps, never diagnostics/prompt assemblies. Every later
      // step invalidates the previous candidate: the final text must be last.
      recovery.phase = 'awaiting-answer'
      recovery.answerStep = event.data.step
      recovery.answerAttempts++
    } else if (event.type === 'assistant/message' && event.data.turn === recovery.turn && event.data.step === recovery.answerStep) {
      recovery.phase = isCompleteAnswer(event.data) ? 'answer-complete' : 'awaiting-answer'
    } else if (event.type === 'assistant/attempt' && event.data.turn === recovery.turn && event.data.step === recovery.answerStep) {
      recovery.phase = 'awaiting-answer'
    }
  })
}

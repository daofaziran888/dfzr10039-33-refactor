import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 调度执行：计划试算/确认、采集轮次与 E5 演练
// ---------------------------------------------------------------------------

export type PlanDerived = {
  计划编号: string
  关联作业: string
  Cron表达式: string
  时区设置: string
  计划时间: string
  计划状态: string
  作业状态: string
  复用计划: boolean
}

export type RoundView = Record<string, unknown>

export const DEMO_BASE_TIME = '2026-11-23T10:00:00+08:00'

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const payload = await response.json()
  if (!response.ok) throw new Error(payload?.detail ?? `接口返回 ${response.status}`)
  return payload as T
}

export async function previewPlan(params: {
  jobId: string
  jobStatus: string
  planId?: string
  cronExpr?: string
  timezone?: string
}): Promise<PlanDerived> {
  const query = new URLSearchParams({ job_id: params.jobId, job_status: params.jobStatus })
  if (params.planId) query.set('plan_id', params.planId)
  if (params.cronExpr) query.set('cron_expr', params.cronExpr)
  if (params.timezone) query.set('timezone_setting', params.timezone)
  const response = await request(`/api/schedule/plan/preview?${query.toString()}`)
  const payload = await response.json()
  if (!response.ok) throw new Error(payload?.detail ?? '计划试算失败')
  return payload.derived as PlanDerived
}

export async function confirmPlan(params: {
  jobId: string
  jobStatus: string
  planId?: string
  cronExpr?: string
  timezone?: string
  clock?: string
}): Promise<{ created: boolean; derived: PlanDerived; plan: Record<string, unknown> }> {
  return await postJson('/api/schedule/plan/confirm', {
    job_id: params.jobId,
    job_status: params.jobStatus,
    plan_id: params.planId || null,
    cron_expr: params.cronExpr || null,
    timezone_setting: params.timezone || null,
    clock: params.clock || null,
  })
}

export async function loadExecutionRounds(): Promise<{ items: RoundView[]; total: number }> {
  const response = await request('/api/crawl_execution?size=200')
  if (!response.ok) throw new Error('采集执行台账读取失败')
  return await response.json()
}

export async function loadRound(roundKey: string): Promise<RoundView> {
  const response = await request(`/api/schedule/round/${encodeURIComponent(roundKey)}`)
  if (!response.ok) throw new Error('轮次明细读取失败')
  return await response.json()
}

export async function claimRound(jobId: string, clock?: string): Promise<RoundView> {
  const payload = await postJson<{ claimed: RoundView; round: RoundView }>(
    '/api/schedule/round/claim', { job_id: jobId, clock: clock || null })
  return payload.round
}

export async function produceRoundBatch(
  roundKey: string,
  batchId?: string,
  clock?: string,
): Promise<{ batch_id: string; row_count: number; round: RoundView }> {
  return await postJson('/api/schedule/round/produce', {
    round_key: roundKey,
    batch_id: batchId || null,
    clock: clock || null,
  })
}

export async function sendInflight(params: {
  jobId: string
  roundKey?: string
  batchId?: string
  entryUrl?: string
  rowCount?: number
}): Promise<{ decision: Record<string, unknown>; round: RoundView | null }> {
  return await postJson('/api/schedule/round/inflight', {
    job_id: params.jobId,
    round_key: params.roundKey || null,
    batch_id: params.batchId || null,
    entry_url: params.entryUrl || null,
    row_count: params.rowCount ?? 0,
  })
}

export async function sendReceipt(params: {
  roundKey: string
  batchId?: string
  interrupt?: boolean
}): Promise<{ interrupted: boolean; round: RoundView }> {
  return await postJson('/api/schedule/round/receipt', {
    round_key: params.roundKey,
    batch_id: params.batchId || null,
    interrupt: params.interrupt ?? false,
  })
}

export async function resumeRound(jobId: string, roundKey?: string): Promise<RoundView> {
  const payload = await postJson<{ claimed: RoundView; round: RoundView }>(
    '/api/schedule/round/resume', { job_id: jobId, round_key: roundKey || null })
  return payload.round
}

export type E5Report = {
  演练: {
    关联作业: string
    固定时钟: string
    轮次: string
    实际入口: string
    原批次: string
    步骤: Record<string, unknown>[]
  }
  原轮: RoundView
  批次: {
    关联作业: string
    轮次数: number
    批次总数: number
    轮次: RoundView[]
    批次: { 批次编号: string; 数据行数: number; 状态: string }[]
  }
  台账: Record<string, unknown>
}

export async function runE5(params: {
  jobId: string
  entryUrl: string
  baseTime?: string
  withNextRound?: boolean
}): Promise<{ report: E5Report; next?: Record<string, unknown> }> {
  const body = {
    job_id: params.jobId,
    entry_url: params.entryUrl,
    base_time: params.baseTime || DEMO_BASE_TIME,
  }
  const path = params.withNextRound ? '/api/schedule/e5/next-round' : '/api/schedule/e5/run'
  return await postJson(path, body)
}


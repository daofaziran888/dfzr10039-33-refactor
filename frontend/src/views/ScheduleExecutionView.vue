<template>
  <section class="page" data-module="schedule_execution">
    <header class="page-head">
      <div>
        <h2>调度执行</h2>
        <p class="page-desc">
          计划执行派生数据、采集轮次台账与 E5 迟到结果 / 补跑演练，固定本地时钟可回放。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="reload-rounds" type="button" @click="reloadRounds">
          刷新执行台账
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="schedule-error">{{ errorMessage }}</p>

    <form class="analysis-form" @submit.prevent="runPreview">
      <label>
        <span>关联作业</span>
        <input v-model="form.jobId" data-testid="plan-job" required />
      </label>
      <label>
        <span>作业状态</span>
        <select v-model="form.jobStatus" data-testid="plan-job-status">
          <option v-for="status in jobStatuses" :key="status" :value="status">{{ status }}</option>
        </select>
      </label>
      <label>
        <span>Cron表达式（选填）</span>
        <input v-model="form.cronExpr" data-testid="plan-cron" placeholder="留空沿用计划或默认" />
      </label>
      <label>
        <span>时区设置（选填）</span>
        <input v-model="form.timezone" data-testid="plan-timezone" placeholder="Asia/Shanghai" />
      </label>
      <button class="btn" data-testid="preview-plan" type="submit">计划试算</button>
      <button class="btn primary" data-testid="confirm-plan" type="button" @click="runConfirm">
        计划确认
      </button>
    </form>

    <article v-if="derived" class="analysis-card" data-testid="plan-derived">
      <strong>计划执行派生数据</strong>
      <dl class="derive-grid">
        <div v-for="(value, key) in derived" :key="key">
          <dt>{{ key }}</dt>
          <dd>{{ value }}</dd>
        </div>
      </dl>
    </article>

    <h3>采集轮次台账</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>轮次</th>
          <th>关联作业</th>
          <th>计划编号</th>
          <th>实际入口</th>
          <th>批次编号</th>
          <th>轮次状态</th>
          <th>回执状态</th>
          <th>计划时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="round in rounds" :key="String(round['轮次'])" :data-testid="`round-${round['轮次']}`">
          <td>{{ round['轮次'] }}</td>
          <td>{{ round['关联作业'] }}</td>
          <td>{{ round['计划编号'] || '—' }}</td>
          <td class="mono">{{ round['实际入口'] }}</td>
          <td>{{ round['批次编号'] || '—' }}</td>
          <td>{{ round['轮次状态'] }}</td>
          <td>{{ round['回执状态'] || '未生成' }}</td>
          <td>{{ round['计划时间'] || '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="selectRound(round)">查轮次</button>
            <button class="link" type="button" @click="resume(round)">补跑本轮</button>
          </td>
        </tr>
        <tr v-if="!rounds.length">
          <td colspan="9" class="empty-state">台账暂无轮次，先运行下方 E5 演练或先确认计划</td>
        </tr>
      </tbody>
    </table>

    <article v-if="selected" class="analysis-card" data-testid="round-detail">
      <strong>轮次明细 {{ selected['轮次'] }}</strong>
      <dl class="derive-grid">
        <div v-for="(value, key) in selectedDetail" :key="key">
          <dt>{{ key }}</dt>
          <dd>{{ formatValue(value) }}</dd>
        </div>
      </dl>
    </article>

    <h3>E5 演练</h3>
    <form class="analysis-form" @submit.prevent="runScenario(true)">
      <label>
        <span>演练作业</span>
        <input v-model="e5Form.jobId" data-testid="e5-job" required />
      </label>
      <label>
        <span>入口URL</span>
        <input v-model="e5Form.entryUrl" data-testid="e5-entry" required />
      </label>
      <label>
        <span>固定时钟</span>
        <input v-model="e5Form.baseTime" data-testid="e5-clock" required />
      </label>
      <button class="btn primary" data-testid="run-e5" type="submit">运行 E5（含下一轮）</button>
    </form>

    <article class="analysis-card" data-testid="e5-manual">
      <strong>分步演练（当前选中轮次）</strong>
      <p v-if="!selected" class="empty-state">先在台账里选中一个轮次</p>
      <div v-else class="page-actions">
        <button class="btn" data-testid="step-withhold" type="button" @click="stepWithhold">
          暂扣执行中结果
        </button>
        <button class="btn" data-testid="step-produce" type="button" @click="stepProduce">
          原批次落盘
        </button>
        <button class="btn" data-testid="step-interrupt" type="button" @click="stepInterrupt">
          断开完成回执
        </button>
        <button class="btn" data-testid="step-resend" type="button" @click="stepResend">
          重送旧结果
        </button>
        <button class="btn" data-testid="step-resume" type="button" @click="stepResume">
          同轮补跑
        </button>
        <button class="btn" data-testid="step-restore" type="button" @click="stepRestore">
          恢复回执
        </button>
        <button class="btn" data-testid="step-next" type="button" @click="stepNextRound">
          下一轮
        </button>
      </div>
    </article>

    <article v-if="report" class="analysis-card" data-testid="e5-report">
      <strong>E5 演练结果</strong>
      <p>
        原轮 <span class="mono">{{ report.演练.轮次 }}</span>，
        实际入口 <span class="mono">{{ report.演练.实际入口 }}</span>，
        原批次 <span class="mono">{{ report.演练.原批次 }}</span>。
      </p>
      <ol class="step-list">
        <li v-for="(step, index) in report.演练.步骤" :key="index">
          <strong>{{ step['步骤'] }}</strong>
          <span class="mono">{{ stepSummary(step) }}</span>
        </li>
      </ol>
      <p v-if="nextRound" data-testid="e5-next">
        下一轮 <span class="mono">{{ nextRound['轮次'] }}</span> 使用
        <span class="mono">{{ nextRound['实际入口'] }}</span> 落批
        <span class="mono">{{ nextRound['批次编号'] }}</span>。
      </p>
    </article>

    <article v-if="stepLog.length" class="analysis-card" data-testid="step-log">
      <strong>分步记录</strong>
      <ol class="step-list">
        <li v-for="(line, index) in stepLog" :key="index" class="mono">{{ line }}</li>
      </ol>
    </article>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  DEMO_BASE_TIME,
  claimRound,
  confirmPlan,
  loadExecutionRounds,
  loadRound,
  previewPlan,
  produceRoundBatch,
  resumeRound,
  runE5,
  sendInflight,
  sendReceipt,
} from '@/api/backend-service'
import type { E5Report, PlanDerived, RoundView } from '@/api/backend-service'

const jobStatuses = ['待执行', '采集执行', '已完成', '部分失败']

const form = reactive({
  jobId: 'CRAW-E5-DEMO',
  jobStatus: '待执行',
  cronExpr: '',
  timezone: '',
})
const e5Form = reactive({
  jobId: 'CRAW-E5-DEMO',
  entryUrl: 'https://e5.example.com/list',
  baseTime: DEMO_BASE_TIME,
})

const derived = ref<PlanDerived | null>(null)
const rounds = ref<RoundView[]>([])
const selected = ref<RoundView | null>(null)
const report = ref<E5Report | null>(null)
const nextRound = ref<Record<string, unknown> | null>(null)
const errorMessage = ref('')

const selectedDetail = computed(() => {
  const row = selected.value
  if (!row) return {}
  const { 迟到结果, ...rest } = row
  return { ...rest, 迟到结果: Array.isArray(迟到结果) ? `${迟到结果.length} 条` : '0 条' }
})

function formatValue(value: unknown): string {
  if (Array.isArray(value)) return value.length ? value.join('；') : '无'
  if (value === true) return '是'
  if (value === false) return '否'
  return value === null || value === undefined || value === '' ? '—' : String(value)
}

function stepSummary(step: Record<string, unknown>): string {
  const ignored = new Set(['步骤'])
  return Object.entries(step)
    .filter(([key]) => !ignored.has(key))
    .map(([key, value]) => `${key}=${formatValue(value)}`)
    .join('，')
}

async function reloadRounds() {
  try {
    const payload = await loadExecutionRounds()
    rounds.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '台账读取失败'
  }
}

async function runPreview() {
  errorMessage.value = ''
  try {
    derived.value = await previewPlan({
      jobId: form.jobId,
      jobStatus: form.jobStatus,
      cronExpr: form.cronExpr || undefined,
      timezone: form.timezone || undefined,
    })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '计划试算失败'
  }
}

async function runConfirm() {
  errorMessage.value = ''
  try {
    const result = await confirmPlan({
      jobId: form.jobId,
      jobStatus: form.jobStatus,
      cronExpr: form.cronExpr || undefined,
      timezone: form.timezone || undefined,
    })
    derived.value = result.derived
    await reloadRounds()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '计划确认失败'
  }
}

async function selectRound(round: RoundView) {
  errorMessage.value = ''
  try {
    selected.value = await loadRound(String(round['轮次']))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '轮次明细读取失败'
  }
}

async function resume(round: RoundView) {
  errorMessage.value = ''
  try {
    selected.value = await resumeRound(String(round['关联作业']), String(round['轮次']))
    await reloadRounds()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补跑失败'
  }
}

async function runScenario(withNextRound: boolean) {
  errorMessage.value = ''
  report.value = null
  nextRound.value = null
  try {
    const result = await runE5({
      jobId: e5Form.jobId,
      entryUrl: e5Form.entryUrl,
      baseTime: e5Form.baseTime,
      withNextRound,
    })
    report.value = result.report
    nextRound.value = (result.next as Record<string, unknown>) ?? null
    if (result.report.原轮) selected.value = result.report.原轮
    await reloadRounds()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'E5 演练失败'
  }
}

// ---------------------------------------------------------------------------
// 分步演练：每一步都打真实接口，便于核对中间状态
// ---------------------------------------------------------------------------

const stepLog = ref<string[]>([])

function currentRound(): Record<string, unknown> | null {
  return selected.value
}

async function refreshSelected() {
  const row = currentRound()
  if (!row) return
  selected.value = await loadRound(String(row['轮次']))
  await reloadRounds()
}

function logStep(name: string, detail?: string) {
  stepLog.value.push(detail ? `${name}：${detail}` : name)
}

async function stepWithhold() {
  const row = currentRound()
  if (!row) return
  try {
    withheldBatch.value = String(row['批次编号'] || `BATCH-${row['轮次']}-LATE`)
    logStep('暂扣执行中结果', String(withheldBatch.value))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '暂扣失败'
  }
}

async function stepProduce() {
  const row = currentRound()
  if (!row) return
  try {
    const result = await produceRoundBatch(String(row['轮次']), withheldBatch.value || undefined)
    logStep('原批次落盘', `${result.batch_id}（${result.row_count} 行）`)
    await refreshSelected()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次落盘失败'
  }
}

async function stepInterrupt() {
  const row = currentRound()
  if (!row) return
  try {
    const result = await sendReceipt({
      roundKey: String(row['轮次']),
      batchId: String(row['批次编号'] || withheldBatch.value || ''),
      interrupt: true,
    })
    logStep('断开完成回执', `轮次=${result.round['轮次状态']} 回执=${result.round['回执状态']}`)
    await refreshSelected()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执中断失败'
  }
}

async function stepResend() {
  const row = currentRound()
  if (!row) return
  try {
    const result = await sendInflight({
      jobId: String(row['关联作业']),
      roundKey: String(row['轮次']),
      batchId: withheldBatch.value || String(row['批次编号'] || ''),
      entryUrl: String(row['实际入口']),
      rowCount: 2,
    })
    const decision = result.decision as Record<string, unknown>
    logStep('重送旧结果', `回填=${decision['applied']}，${decision['reason']}`)
    await refreshSelected()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重送结果失败'
  }
}

async function stepResume() {
  const row = currentRound()
  if (!row) return
  try {
    selected.value = await resumeRound(String(row['关联作业']), String(row['轮次']))
    noteResume()
    await reloadRounds()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补跑失败'
  }
}

function noteResume() {
  const row = currentRound()
  if (!row) return
  logStep('同轮补跑', `入口=${row['实际入口']}，批次=${row['批次编号'] || '无'}，批次可查=${row['批次可查']}`)
}

async function stepRestore() {
  const row = currentRound()
  if (!row) return
  try {
    const result = await sendReceipt({
      roundKey: String(row['轮次']),
      batchId: String(row['批次编号'] || ''),
      interrupt: false,
    })
    logStep('恢复回执', `回执=${result.round['回执状态']}，序号=${result.round['回执序号']}`)
    await refreshSelected()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执恢复失败'
  }
}

async function stepNextRound() {
  const row = currentRound()
  if (!row) return
  try {
    const nextClaim = await claimRound(String(row['关联作业']), e5Form.baseTime)
    const produced = await produceRoundBatch(String(nextClaim['轮次']))
    await sendReceipt({
      roundKey: String(nextClaim['轮次']),
      batchId: produced.batch_id,
      interrupt: false,
    })
    logStep('下一轮落批', `${nextClaim['轮次']} 入口=${nextClaim['实际入口']} 批次=${produced.batch_id}`)
    await reloadRounds()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下一轮失败'
  }
}

const withheldBatch = ref('')

onMounted(reloadRounds)
</script>

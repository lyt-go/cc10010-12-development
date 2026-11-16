<template>
  <section class="page" data-module="rerun">
    <header class="page-head">
      <div>
        <h2>异常重跑管理</h2>
        <p class="page-desc">消费运行水位与断点数据快照，异常重跑只从中断分区继续，读取/写入行数可回查。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记重跑任务</button>
        <button class="btn" type="button" @click="exportRows">导出异常重跑清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="runAction('提交重跑审批', row)">提交重跑审批</button>
            <button class="link" type="button" @click="runAction('启动重跑', row)">启动重跑</button>
            <button class="link" type="button" @click="runAction('取消重跑', row)">取消重跑</button>
            <button class="link" type="button" @click="openProgress(row)">恢复进度</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无异常重跑数据，可先登记重跑任务</td>
        </tr>
      </tbody>
    </table>

    <div v-if="selected" class="detail-panel">
      <div class="page-head">
        <h3>断点恢复进度 · {{ selected['任务编号'] }}</h3>
        <button class="btn ghost" type="button" @click="closeDetail">关闭</button>
      </div>

      <div v-if="progress && progress.resume_partition_total > 0" class="detail-grid">
        <div><span>恢复起点（断点分区）</span>{{ progress.resume_from_partition ?? '—' }}</div>
        <div><span>恢复分区数</span>{{ progress.finished_partition_count }}/{{ progress.resume_partition_total }}</div>
        <div><span>重跑累计读取行数</span>{{ progress.read_rows }}</div>
        <div><span>重跑累计写入行数</span>{{ progress.write_rows }}</div>
        <div><span>是否全部恢复</span>{{ progress.finished ? '已完成' : '进行中' }}</div>
      </div>
      <p v-else class="empty-state">{{ progress?.note ?? '该重跑任务尚未启动或未消费分区水位。' }}</p>

      <div v-if="progress && progress.resume_partition_total > 0" class="partition-grid">
        <div
          v-for="part in progress.partitions"
          :key="part.partition_no"
          :class="['partition-card', `state-${part.state}`]"
        >
          <strong>{{ part.partition_no }} · {{ part.state }}</strong>
          <div>读取 {{ part.read_rows }} / 写入 {{ part.write_rows }}</div>
        </div>
      </div>

      <template v-if="selected.status === '执行中'">
        <h3>上报重跑分区进度</h3>
        <form class="report-form" @submit.prevent="submitRerunEvent">
          <label><span>分区编号</span>
            <select v-model="rerunForm.partition_no">
              <option v-for="no in resumeOptions" :key="no" :value="no">{{ no }}</option>
            </select>
          </label>
          <label><span>单调序号 seq</span><input v-model.number="rerunForm.seq" type="number" min="1" required /></label>
          <label><span>分区结果</span>
            <select v-model="rerunForm.result">
              <option value="成功">成功</option>
              <option value="失败">失败</option>
            </select>
          </label>
          <label><span>读取行数</span><input v-model.number="rerunForm.read_rows" type="number" min="0" required /></label>
          <label><span>写入行数</span><input v-model.number="rerunForm.write_rows" type="number" min="0" required /></label>
          <label><span>请求ID（幂等）</span><input v-model="rerunForm.request_id" placeholder="req-xxx" required /></label>
          <button class="btn primary" type="submit">提交重跑事件</button>
        </form>
        <p v-if="eventHint" :class="eventHintOk ? 'ok-text' : 'error-text'">{{ eventHint }}</p>
      </template>

      <div class="event-list">
        <table class="data-table">
          <thead>
            <tr><th>分区</th><th>seq</th><th>结果</th><th>读取</th><th>写入</th><th>请求ID</th><th>结论</th><th>说明 / 时间</th></tr>
          </thead>
          <tbody>
            <tr v-for="event in events" :key="String(event.id)">
              <td>{{ event.partition_no }}</td>
              <td>{{ event.seq }}</td>
              <td>{{ event.result }}</td>
              <td>{{ event.read_rows }}</td>
              <td>{{ event.write_rows }}</td>
              <td>{{ event.request_id }}</td>
              <td><span :class="['tag', event.effective ? 'tag-applied' : 'tag-conflict']">
                {{ event.effective ? '已生效' : (event.reject_reason?.includes('取代') ? '已被取代' : '冲突拒绝') }}
              </span></td>
              <td>{{ event.reject_reason ?? event.created_at }}</td>
            </tr>
            <tr v-if="!events.length">
              <td colspan="8" class="empty-state">暂无重跑分区事件</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条异常重跑记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

interface RerunPartition {
  partition_no: string
  state: string
  read_rows: number
  write_rows: number
}

interface RerunProgress {
  resume_from_partition: string | null
  resume_partitions: string[]
  resume_partition_total: number
  finished_partition_count: number
  read_rows: number
  write_rows: number
  finished: boolean
  partitions: RerunPartition[]
  note?: string
}

interface RerunEvent {
  id: number
  partition_no: string
  seq: number
  result: string
  read_rows: number
  write_rows: number
  request_id: string
  effective: boolean
  reject_reason: string | null
  created_at: string
}

const ENDPOINT = '/api/rerun'
const columns = ['任务编号', '原始运行编号', '重跑区间开始', '重跑区间结束', '重跑原因', '覆盖策略', '重跑状态', '断点分区', '恢复分区数', '读取行数', '写入行数']
const stats = [{ label: '重跑任务总数', value: 0 }, { label: '执行中任务', value: 0 }, { label: '已完成任务', value: 0 }]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = ['任务编号', '原始运行编号', '重跑区间开始']

const selected = ref<Row | null>(null)
const progress = ref<RerunProgress | null>(null)
const events = ref<RerunEvent[]>([])
const eventHint = ref('')
const eventHintOk = ref(true)
const rerunForm = reactive({
  partition_no: '',
  seq: 1,
  result: '成功',
  read_rows: 0,
  write_rows: 0,
  request_id: '',
})

const resumeOptions = computed(() => progress.value?.resume_partitions ?? [])

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '重跑任务登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.message ?? '异常重跑动作未生效，请稍后重试')
    }
    errorMessage.value = action === '启动重跑' ? payload.message : ''
    await reload()
    if (selected.value && String(selected.value.id) === String(row.id)) {
      await openProgress(payload.entry ?? row)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '异常重跑操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('重跑任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    stats[0].value = total.value
    stats[1].value = rows.value.filter((row) => row.status === '执行中').length
    stats[2].value = rows.value.filter((row) => row.status === '已完成').length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '异常重跑列表读取失败'
  }
}

async function openProgress(row: Row) {
  selected.value = row
  eventHint.value = ''
  rerunForm.partition_no = ''
  await loadProgress()
  await loadEvents()
  if (!rerunForm.partition_no && resumeOptions.value.length > 0) {
    rerunForm.partition_no = resumeOptions.value[0]
  }
}

function closeDetail() {
  selected.value = null
  progress.value = null
  events.value = []
}

async function loadProgress() {
  if (!selected.value) return
  try {
    const response = await request(`${ENDPOINT}/${selected.value.id}/progress`)
    if (!response.ok) {
      throw new Error('重跑恢复进度读取失败')
    }
    progress.value = (await response.json()) as RerunProgress
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重跑恢复进度读取失败'
  }
}

async function loadEvents() {
  if (!selected.value) return
  try {
    const response = await request(`${ENDPOINT}/${selected.value.id}/partition-events`)
    if (!response.ok) {
      throw new Error('重跑分区事件读取失败')
    }
    const payload = await response.json()
    events.value = (payload.items ?? []) as RerunEvent[]
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重跑分区事件读取失败'
  }
}

async function submitRerunEvent() {
  if (!selected.value) return
  try {
    const response = await request(`${ENDPOINT}/${selected.value.id}/partition-events`, {
      method: 'POST',
      body: JSON.stringify({
        values: {
          partition_no: rerunForm.partition_no,
          seq: rerunForm.seq,
          result: rerunForm.result,
          read_rows: rerunForm.read_rows,
          write_rows: rerunForm.write_rows,
          request_id: rerunForm.request_id,
        },
      }),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.message ?? '重跑分区事件提交失败')
    }
    eventHintOk.value = true
    eventHint.value = payload.message
    await loadProgress()
    await loadEvents()
    await reload()
    selected.value = rows.value.find((row) => String(row.id) === String(selected.value?.id)) ?? selected.value
  } catch (error) {
    eventHintOk.value = false
    eventHint.value = error instanceof Error ? error.message : '重跑分区事件提交失败'
  }
}

onMounted(reload)
</script>

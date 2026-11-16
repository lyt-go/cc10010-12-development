<template>
  <section class="page" data-module="runtime">
    <header class="page-head">
      <div>
        <h2>运行实例管理</h2>
        <p class="page-desc">记录每次ETL作业的运行实例，追踪执行状态、分区级运行水位与断点交接。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记运行记录</button>
        <button class="btn" type="button" @click="exportRows">导出运行实例清单</button>
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
            <button class="link" type="button" @click="runAction('启动作业', row)">启动作业</button>
            <button class="link" type="button" @click="runAction('终止作业', row)">终止作业</button>
            <button class="link" type="button" @click="runAction('标记成功', row)">标记成功</button>
            <button class="link" type="button" @click="openWatermark(row)">分区水位</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无运行实例数据，可先登记运行记录</td>
        </tr>
      </tbody>
    </table>

    <div v-if="selected" class="detail-panel">
      <div class="page-head">
        <h3>运行水位与断点数据快照 · {{ selected['记录编号'] }}</h3>
        <button class="btn ghost" type="button" @click="closeDetail">关闭</button>
      </div>

      <div v-if="watermark" class="detail-grid">
        <div><span>执行状态</span>{{ watermark.state }}</div>
        <div><span>最后稳定分区</span>{{ watermark.last_stable_partition ?? '—' }}</div>
        <div><span>断点分区</span>{{ watermark.breakpoint_partition ?? '无（已全部成功）' }}</div>
        <div><span>分区完成数</span>{{ watermark.success_partition_count }}/{{ watermark.partition_total }}</div>
        <div><span>累计读取行数</span>{{ watermark.read_rows }}</div>
        <div><span>累计写入行数</span>{{ watermark.write_rows }}</div>
        <div><span>水位版本</span>v{{ watermark.version }}</div>
        <div><span>最近更新时间</span>{{ watermark.updated_at ?? '—' }}</div>
      </div>
      <p v-else class="empty-state">该运行记录还没有分区进度事件，先在下方上报第一条事件。</p>

      <div v-if="watermark" class="partition-grid">
        <div
          v-for="part in watermark.partitions"
          :key="part.partition_no"
          :class="['partition-card', `state-${part.state}`]"
        >
          <strong>{{ part.partition_no }} · {{ part.state }}</strong>
          <div>序号 seq：{{ part.seq ?? '—' }}</div>
          <div>读取 {{ part.read_rows }} / 写入 {{ part.write_rows }}</div>
        </div>
      </div>

      <h3>上报运行分区进度</h3>
      <form class="report-form" @submit.prevent="submitPartitionEvent">
        <label><span>分区编号</span><input v-model="eventForm.partition_no" placeholder="p001" required /></label>
        <label><span>单调序号 seq</span><input v-model.number="eventForm.seq" type="number" min="1" required /></label>
        <label><span>分区结果</span>
          <select v-model="eventForm.result">
            <option value="成功">成功</option>
            <option value="失败">失败</option>
          </select>
        </label>
        <label><span>读取行数</span><input v-model.number="eventForm.read_rows" type="number" min="0" required /></label>
        <label><span>写入行数</span><input v-model.number="eventForm.write_rows" type="number" min="0" required /></label>
        <label><span>请求ID（幂等）</span><input v-model="eventForm.request_id" placeholder="req-xxx" required /></label>
        <label v-if="!watermark"><span>总分区数（首次必填）</span>
          <input v-model.number="eventForm.partition_total" type="number" min="1" />
        </label>
        <button class="btn primary" type="submit">提交分区事件</button>
      </form>
      <p v-if="eventHint" :class="eventHintOk ? 'ok-text' : 'error-text'">{{ eventHint }}</p>

      <div class="page-actions" style="margin-top: 12px">
        <button class="btn" type="button" @click="reopenProcess">进程断线重连（交接最后稳定分区）</button>
        <button class="btn ghost" type="button" @click="loadEvents">刷新事件历史</button>
      </div>

      <div class="event-list">
        <table class="data-table">
          <thead>
            <tr>
              <th>分区</th><th>seq</th><th>结果</th><th>读取</th><th>写入</th>
              <th>请求ID</th><th>结论</th><th>说明 / 时间</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="event in events" :key="String(event.id)">
              <td>{{ event.partition_no }}</td>
              <td>{{ event.seq }}</td>
              <td>{{ event.result }}</td>
              <td>{{ event.read_rows }}</td>
              <td>{{ event.write_rows }}</td>
              <td>{{ event.request_id }}</td>
              <td><span :class="['tag', eventTagClass(event)]">{{ eventTagText(event) }}</span></td>
              <td>{{ event.reject_reason ?? event.created_at }}</td>
            </tr>
            <tr v-if="!events.length">
              <td colspan="8" class="empty-state">暂无分区进度事件</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条运行实例记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

interface PartitionState {
  partition_no: string
  state: string
  seq: number | null
  read_rows: number
  write_rows: number
}

interface Watermark {
  state: string
  last_stable_partition: string | null
  breakpoint_partition: string | null
  success_partition_count: number
  partition_total: number
  read_rows: number
  write_rows: number
  version: number
  updated_at: string | null
  partitions: PartitionState[]
}

interface PartitionEvent {
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

const ENDPOINT = '/api/runtime'
const columns = ['记录编号', '作业名称', '开始时间', '执行状态', '读取行数', '写入行数', '最后稳定分区', '断点分区', '分区完成数', '错误信息']
const stats = [{ label: '运行记录总数', value: 0 }, { label: '今日运行数', value: 0 }, { label: '失败记录数', value: 0 }]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = ['记录编号', '作业名称', '开始时间']

const selected = ref<Row | null>(null)
const watermark = ref<Watermark | null>(null)
const events = ref<PartitionEvent[]>([])
const eventHint = ref('')
const eventHintOk = ref(true)
const eventForm = reactive({
  partition_no: '',
  seq: 1,
  result: '成功',
  read_rows: 0,
  write_rows: 0,
  request_id: '',
  partition_total: undefined as number | undefined,
})

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '运行记录登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    if (!response.ok) {
      throw new Error('运行实例动作未生效，请稍后重试')
    }
    await reload()
    if (selected.value && String(selected.value.id) === String(row.id)) {
      await loadWatermark()
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '运行实例操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('运行记录列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    stats[0].value = total.value
    stats[2].value = rows.value.filter((row) => row['执行状态'] === '失败').length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '运行实例列表读取失败'
  }
}

async function openWatermark(row: Row) {
  selected.value = row
  eventHint.value = ''
  await loadWatermark()
  await loadEvents()
}

function closeDetail() {
  selected.value = null
  watermark.value = null
  events.value = []
}

async function loadWatermark() {
  if (!selected.value) return
  try {
    const response = await request(`${ENDPOINT}/${selected.value.id}/watermark`)
    if (response.status === 404) {
      watermark.value = null
      return
    }
    if (!response.ok) {
      throw new Error('运行水位读取失败')
    }
    watermark.value = (await response.json()) as Watermark
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '运行水位读取失败'
  }
}

async function loadEvents() {
  if (!selected.value) return
  try {
    const response = await request(`${ENDPOINT}/${selected.value.id}/partition-events`)
    if (!response.ok) {
      throw new Error('分区事件历史读取失败')
    }
    const payload = await response.json()
    events.value = (payload.items ?? []) as PartitionEvent[]
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区事件历史读取失败'
  }
}

async function submitPartitionEvent() {
  if (!selected.value) return
  const values: Record<string, string | number> = {
    partition_no: eventForm.partition_no,
    seq: eventForm.seq,
    result: eventForm.result,
    read_rows: eventForm.read_rows,
    write_rows: eventForm.write_rows,
    request_id: eventForm.request_id,
  }
  if (eventForm.partition_total !== undefined) {
    values.partition_total = eventForm.partition_total
  }
  try {
    const response = await request(`${ENDPOINT}/${selected.value.id}/partition-events`, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.message ?? '分区事件提交失败')
    }
    eventHintOk.value = true
    eventHint.value = payload.message
    await loadWatermark()
    await loadEvents()
    await reload()
  } catch (error) {
    eventHintOk.value = false
    eventHint.value = error instanceof Error ? error.message : '分区事件提交失败'
  }
}

async function reopenProcess() {
  if (!selected.value) return
  try {
    const response = await request(`${ENDPOINT}/${selected.value.id}/process-sessions`, {
      method: 'POST',
      body: JSON.stringify({ values: { process_id: `worker-${Date.now()}` } }),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.message ?? '进程重连失败')
    }
    eventHintOk.value = true
    eventHint.value = `${payload.message}：从 ${payload.entry.session.resume_from_partition ?? '下一分区'} 继续`
  } catch (error) {
    eventHintOk.value = false
    eventHint.value = error instanceof Error ? error.message : '进程重连失败'
  }
}

function eventTagClass(event: PartitionEvent): string {
  if (!event.effective) {
    return event.reject_reason?.includes('取代') ? 'tag-duplicate' : 'tag-conflict'
  }
  return 'tag-applied'
}

function eventTagText(event: PartitionEvent): string {
  if (event.effective) return '已生效'
  return event.reject_reason?.includes('取代') ? '已被取代' : '冲突拒绝'
}

onMounted(reload)
</script>

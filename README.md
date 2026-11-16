# 数据仓库ETL任务编排平台

面向数据仓库ETL任务开发、作业编排调度、任务依赖解析、源端数据校验、异常任务重跑、血缘分板与运行全景监控的ETL工程管理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| ETL作业 | `etljob` | 作业定义 | 作业编号、作业名称、源端数据表 |
| 任务编排 | `dag` | 编排图 | 图编号、图名称、拓扑结构 |
| 依赖解析 | `dependency` | 依赖关系 | 关系编号、上游作业、下游作业 |
| 源端校验 | `sourcecheck` | 校验配置 | 配置编号、源端数据表、行数下限 |
| 运行实例 | `runtime` | 运行记录 | 记录编号、作业名称、开始时间 |
| 异常重跑 | `rerun` | 重跑任务 | 任务编号、原始运行编号、重跑区间开始 |
| 基线管理 | `baseline` | 基线定义 | 基线编号、基线名称、承诺完成时间 |
| 分区检测 | `partitioncheck` | 检测规则 | 规则编号、数据表名、期望分区数 |
| 数据比对 | `comparator` | 比对任务 | 任务编号、源表名、目标表名 |
| 元数据存储 | `metastore` | 元数据表 | 表编号、表名、所属数仓层 |
| 资源配置 | `resource` | 资源池 | 池编号、池名称、最大并发数 |
| SLA监控 | `slavalidate` | SLA规则 | 规则编号、适用作业、完成截止时间 |
| SQL模板 | `sqltpl` | 模板定义 | 模板编号、模板名称、模板类型 |
| 数据备份 | `backuprestore` | 备份策略 | 策略编号、数据表名、备份周期 |
| 日志归档 | `logarchive` | 归档策略 | 策略编号、日志类型、保留天数 |
| 通知渠道 | `notifychan` | 渠道配置 | 渠道编号、渠道类型、接收地址 |
| 发布计划 | `publishplan` | 发布单 | 发布编号、发布内容、变更说明 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 分区级运行水位与断点交接

运行实例不再只有整体状态，而是按分区维护运行水位与断点，异常重跑只从中断分区继续。

### 数据与规则（`app/services/partition.py`）

- **运行分区进度事件**（内部表 `runtime_partition_event`，只追加不删改）：
  字段含 `partition_no`（p001 形式）、单调序号 `seq`、`result`（成功/失败）、
  `read_rows`、`write_rows`、`request_id`。
- **运行水位与断点数据快照**（内部表 `runtime_watermark`，每次事件推进一个 `version`）：
  - 最后稳定分区：从 p001 起连续成功的最长前缀末位；
  - 断点分区：第一个未成功的分区（失败或尚未上报），全部成功则为空；
  - 累计读取/写入行数由各分区最新生效事件汇总。
- **同请求重复提交只生效一次**：相同 `request_id` 直接幂等返回 `duplicate`，行数不重复累计。
- **跨进程迟到事件冲突拒绝**：同分区只接受更大的 `seq`；旧 `seq` 事件标记
  `effective=false` 并写明拒绝原因，历史记录保留，水位绝不回退。被新 `seq` 取代的
  旧事件同样留痕但不再计入水位。
- **同一事务落库**：事件、水位、读取/写入行数与执行状态在 `store.transaction()`
  内一起提交，任何一步抛异常整体回滚（深拷贝快照 + 锁，模拟数据库本地事务）。
- **进程断线重连**：`process-sessions` 交接最后稳定分区，新进程从断点分区继续。

### 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/runtime/{id}/partition-events` | 上报运行分区进度，返回 `outcome`（applied/duplicate/conflict）与最新水位 |
| GET | `/api/runtime/{id}/watermark` | 读取运行水位与断点数据快照 |
| GET | `/api/runtime/{id}/partition-events` | 分区事件全量历史（含被拒绝、被取代事件） |
| POST | `/api/runtime/{id}/process-sessions` | 进程断线重连，交接最后稳定分区与恢复起点 |
| POST | `/api/rerun/{id}/actions`（启动重跑） | 消费水位快照，冻结重跑范围为未完成分区 |
| POST | `/api/rerun/{id}/partition-events` | 上报重跑分区进度，范围外分区一律拒绝 |
| GET | `/api/rerun/{id}/progress` | 重跑恢复进度与累计读取/写入行数 |
| GET | `/api/rerun/{id}/partition-events` | 重跑分区事件历史，行数可逐条回查 |

### 重跑恢复口径

- 启动重跑时把水位快照（含断点分区、未完成分区列表、当时行数、`watermark_version`）
  冻结到重跑任务上，之后原始运行再迟到事件也不会改变本次重跑写入范围；
- 重跑只接受快照圈定的未完成分区，已成功分区不在写入范围；
- 历史运行没有分区水位时保留旧口径（按重跑区间整段覆盖），不阻断老流程；
- 运行实例刷新、返回、重新进入详情或再次读取时，执行状态与水位由同一份快照同步，
  原断点与读取/写入行数保留且可核对。

### 验收测试

```bash
cd backend
.venv/bin/python -m unittest discover -s tests     # 16 个单元用例
.venv/bin/python tests/http_smoke.py              # 真实 HTTP 端到端冒烟
```

领域：数据工程
子方向：数仓与数据湖

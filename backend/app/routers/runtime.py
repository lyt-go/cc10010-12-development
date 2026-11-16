"""运行实例接口：维护运行记录，覆盖启动作业、终止作业、标记成功等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.runtime import RuntimeService

router = APIRouter(prefix="/api/runtime", tags=["运行实例"])

service = RuntimeService()

LIST_FIELDS = ["记录编号", "作业名称", "开始时间", "结束时间", "执行状态", "读取行数", "写入行数", "错误信息"]
STATUSES = ["草稿", "运行中", "成功", "失败", "已终止"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按记录编号检索"),
    status: str | None = Query(default=None, description="草稿、运行中、成功、失败、已终止"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按记录编号与状态过滤运行实例列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出运行实例清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "runtime", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条运行记录明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条运行记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="运行记录已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条运行记录执行启动作业、终止作业、标记成功；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/partition-events", response_model=ActionResult)
def report_partition_event(entry_id: int, payload: EntryPayload) -> ActionResult:
    """上报运行分区进度事件，汇总运行水位与断点数据快照。

    同 request_id 重复提交只生效一次；迟到旧分区事件按分区单调序号拒绝并留痕；
    水位、断点、读取/写入行数与执行状态在同一事务内落库。
    """
    result, message = service.report_partition(entry_id, payload.values)
    if result is None:
        return ActionResult(ok=False, message=message)
    # 冲突与重复属于业务上被接受但未推进水位的结果，HTTP 仍返回 200，靠 outcome 区分。
    return ActionResult(ok=True, message=message, entry=result)


@router.get("/{entry_id}/watermark", response_model=dict)
def get_watermark(entry_id: int) -> dict[str, Any]:
    """读取运行水位与断点数据快照，供异常重跑与断线重连消费。"""
    snapshot = service.get_watermark(entry_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {entry_id} 不存在或尚无分区水位")
    return snapshot


@router.get("/{entry_id}/partition-events", response_model=dict)
def list_partition_events(entry_id: int) -> dict[str, Any]:
    """返回全部分区进度事件（含重复、被拒绝的迟到事件），历史运行记录可核对。"""
    events = service.list_partition_events(entry_id)
    if events is None:
        raise HTTPException(status_code=404, detail=f"运行记录 {entry_id} 不存在或已归档")
    return {"run_id": entry_id, "total": len(events), "items": events}


@router.post("/{entry_id}/process-sessions", response_model=ActionResult)
def reopen_process(entry_id: int, payload: EntryPayload) -> ActionResult:
    """进程断线后重新开始：交接最后稳定分区，新进程从断点分区继续。"""
    result, message = service.reopen_process(entry_id, payload.values)
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)

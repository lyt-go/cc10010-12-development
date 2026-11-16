"""异常重跑接口：维护重跑任务，覆盖提交重跑审批、启动重跑、取消重跑等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.rerun import RerunService

router = APIRouter(prefix="/api/rerun", tags=["异常重跑"])

service = RerunService()

LIST_FIELDS = ["任务编号", "原始运行编号", "重跑区间开始", "重跑区间结束", "重跑原因", "覆盖策略", "依赖级联", "重跑状态"]
STATUSES = ["草稿", "重跑审批中", "执行中", "已完成", "已取消"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="草稿、重跑审批中、执行中、已完成、已取消"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤异常重跑列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出异常重跑清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "rerun", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条重跑任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"重跑任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条重跑任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="重跑任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条重跑任务执行提交重跑审批、启动重跑、取消重跑；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/partition-events", response_model=ActionResult)
def report_partition_event(entry_id: int, payload: EntryPayload) -> ActionResult:
    """上报重跑分区进度：只接受断点快照圈定的未完成分区，行数同事务累计可回查。"""
    result, message = service.report_partition(entry_id, payload.values)
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


@router.get("/{entry_id}/progress", response_model=dict)
def get_progress(entry_id: int) -> dict[str, Any]:
    """读取重跑断点恢复进度与累计读取、写入行数。"""
    progress = service.get_progress(entry_id)
    if progress is None:
        raise HTTPException(status_code=404, detail=f"重跑任务 {entry_id} 不存在或已归档")
    return progress


@router.get("/{entry_id}/partition-events", response_model=dict)
def list_partition_events(entry_id: int) -> dict[str, Any]:
    """返回重跑全部分区进度事件（含重复、被拒绝的迟到事件），行数可逐条核对。"""
    events = service.list_partition_events(entry_id)
    if events is None:
        raise HTTPException(status_code=404, detail=f"重跑任务 {entry_id} 不存在或已归档")
    return {"rerun_id": entry_id, "total": len(events), "items": events}

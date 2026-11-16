"""运行实例业务规则：状态流转、字段校验、分区水位与断点交接都收在这里。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services import partition
from app.store import store

MODULE = "runtime"
REQUIRED_FIELDS = ["记录编号", "作业名称", "开始时间"]
STATUS_ORDER = ["草稿", "运行中", "成功", "失败", "已终止"]
ACTION_RULES = {"启动作业": "运行中", "终止作业": "已终止", "标记成功": "成功"}
NEGATIVE_ACTIONS = []

# 水位状态 -> 运行实例执行状态，保证刷新、返回、重新进入详情时两边口径一致。
WATERMARK_STATUS_MAP = {"运行中": "运行中", "成功": "成功", "失败": "失败"}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class RuntimeService:
    def _sync_entry_with_watermark(self, entry: dict[str, Any]) -> dict[str, Any]:
        """把水位快照同步到运行记录：执行状态、读取/写入行数、断点字段一致且保留原值。

        每次列表、详情、动作返回前都会调用，因此刷新页面或重新进入详情时，
        看到的执行状态与分区水位始终是同一份汇总结果。
        """
        snapshot = partition.summarize_watermark(int(entry["id"]))
        if snapshot is None:
            return entry
        entry["执行状态"] = WATERMARK_STATUS_MAP[snapshot["state"]]
        entry["status"] = entry["执行状态"]
        # 原读取/写入行数字段保留为可核对口径，数值由分区事件累计得出。
        entry["读取行数"] = snapshot["read_rows"]
        entry["写入行数"] = snapshot["write_rows"]
        entry["最后稳定分区"] = snapshot["last_stable_partition"]
        entry["断点分区"] = snapshot["breakpoint_partition"]
        entry["分区完成数"] = f"{snapshot['success_partition_count']}/{snapshot['partition_total']}"
        entry["水位版本"] = snapshot["version"]
        entry["abnormal"] = snapshot["state"] == "失败"
        entry["pending"] = snapshot["state"] != "成功"
        return entry

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        with store.locked():
            rows = store.rows(MODULE)
            for entry in rows:
                self._sync_entry_with_watermark(entry)
            if keyword:
                rows = [row for row in rows if keyword in str(row.get("记录编号", ""))]
            if status:
                rows = [row for row in rows if row.get("status") == status]
            total = len(rows)
            start = max(page - 1, 0) * size
            return [dict(row) for row in rows[start:start + size]], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        with store.locked():
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None
            self._sync_entry_with_watermark(entry)
            return dict(entry)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        with store.transaction():
            rows = store.rows(MODULE)
            entry = {"id": store.next_id(MODULE)}
            entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
            entry["status"] = STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            rows.append(entry)
            return dict(entry), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        with store.transaction():
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"运行记录 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于运行实例可执行范围"
            target = ACTION_RULES[action]
            if target not in STATUS_ORDER:
                return None, f"目标状态「{target}」不在允许的状态序列里"
            entry["status"] = target
            entry["执行状态"] = target
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            # 已建立分区水位的记录以水位为准，返回前再同步一次，保证动作响应与
            # 后续刷新、重新进入详情看到的执行状态完全一致。
            self._sync_entry_with_watermark(entry)
            return dict(entry), f"运行记录已{action}"

    # ------------------------------------------------------------------
    # 分区级运行水位与断点交接
    # ------------------------------------------------------------------

    def report_partition(
        self, entry_id: int, payload: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        """接收运行分区进度事件，事务内同时落库事件、水位快照、行数与执行状态。"""
        try:
            with store.transaction():
                entry = store.find(MODULE, entry_id)
                if entry is None:
                    return None, f"运行记录 {entry_id} 不存在或已归档"
                values = dict(payload)
                values.setdefault("record_no", entry.get("记录编号"))
                # ValueError 穿透 with 触发整事务回滚，不会留下半截水位。
                event, outcome = partition.receive_runtime_event(entry_id, values, _now())
                snapshot = partition.summarize_watermark(entry_id)
                # 同一事务内把执行状态、读取/写入行数与水位对齐，杜绝两边不一致。
                self._sync_entry_with_watermark(entry)
                message = {
                    partition.OUTCOME_APPLIED: "分区进度已生效，运行水位与断点已刷新",
                    partition.OUTCOME_DUPLICATE: "同请求分区进度已接收过，本次幂等忽略，水位保持不变",
                    partition.OUTCOME_CONFLICT: f"迟到旧分区事件已拒绝：{event.get('reject_reason')}",
                }[outcome]
                return {
                    "outcome": outcome,
                    "message": message,
                    "event": event,
                    "watermark": snapshot,
                    "entry": dict(entry),
                }, message
        except ValueError as exc:
            return None, str(exc)

    def get_watermark(self, entry_id: int) -> dict[str, Any] | None:
        """返回运行水位与断点数据快照；无快照时给 None，由路由层转 404。"""
        with store.locked():
            if store.find(MODULE, entry_id) is None:
                return None
            return partition.summarize_watermark(entry_id)

    def list_partition_events(self, entry_id: int) -> list[dict[str, Any]] | None:
        with store.locked():
            if store.find(MODULE, entry_id) is None:
                return None
            return partition.list_events(entry_id)

    def reopen_process(
        self, entry_id: int, payload: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        """进程断线后重新开始：登记新进程会话，交接最后稳定分区与恢复起点。"""
        process_id = str(payload.get("process_id") or "").strip()
        if not process_id:
            return None, "process_id 不能为空"
        try:
            with store.transaction():
                entry = store.find(MODULE, entry_id)
                if entry is None:
                    return None, f"运行记录 {entry_id} 不存在或已归档"
                session = partition.open_process_session(entry_id, process_id, _now())
                snapshot = partition.summarize_watermark(entry_id)
                return {"session": session, "watermark": snapshot}, "进程已重新接入，可从断点分区继续"
        except ValueError as exc:
            return None, str(exc)

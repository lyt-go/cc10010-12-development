"""异常重跑业务规则：状态流转、字段校验、按运行水位断点恢复未完成分区。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services import partition
from app.store import store

MODULE = "rerun"
RUNTIME_MODULE = "runtime"
REQUIRED_FIELDS = ["任务编号", "原始运行编号", "重跑区间开始"]
STATUS_ORDER = ["草稿", "重跑审批中", "执行中", "已完成", "已取消"]
ACTION_RULES = {"提交重跑审批": "重跑审批中", "启动重跑": "执行中", "取消重跑": "已取消"}
NEGATIVE_ACTIONS = []


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class RerunService:
    def _attach_progress(self, entry: dict[str, Any]) -> dict[str, Any]:
        """把断点恢复进度与读取、写入行数挂到重跑记录上，刷新页面即可核对。"""
        if entry.get("resume_partitions"):
            progress = partition.summarize_rerun(entry)
            entry["断点分区"] = progress["resume_from_partition"]
            entry["恢复分区数"] = f"{progress['finished_partition_count']}/{progress['resume_partition_total']}"
            entry["读取行数"] = progress["read_rows"]
            entry["写入行数"] = progress["write_rows"]
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
                self._attach_progress(entry)
            if keyword:
                rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
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
            self._attach_progress(entry)
            return dict(entry)

    def _find_runtime(self, record_no: str) -> dict[str, Any] | None:
        for row in store.rows(RUNTIME_MODULE):
            if str(row.get("记录编号")) == str(record_no):
                return row
        return None

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
            # 预读水位，登记时就提示本次重跑会从哪个断点继续，真正冻结发生在启动重跑。
            runtime = self._find_runtime(str(entry["原始运行编号"]))
            snapshot = partition.summarize_watermark(int(runtime["id"])) if runtime else None
            if snapshot is not None:
                entry["断点分区"] = snapshot["breakpoint_partition"]
            rows.append(entry)
            return dict(entry), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        try:
            with store.transaction():
                entry = store.find(MODULE, entry_id)
                if entry is None:
                    return None, f"重跑任务 {entry_id} 不存在或已归档"
                if action not in ACTION_RULES:
                    return None, f"动作「{action}」不属于异常重跑可执行范围"
                target = ACTION_RULES[action]
                if target not in STATUS_ORDER:
                    return None, f"目标状态「{target}」不在允许的状态序列里"

                if action == "启动重跑":
                    # 水位缺失、无断点等 ValueError 穿透 with 触发整事务回滚。
                    message = self._start_from_watermark(entry)
                else:
                    entry["status"] = target
                    entry["pending"] = target != STATUS_ORDER[-1]
                    entry["abnormal"] = action in NEGATIVE_ACTIONS
                    message = f"重跑任务已{action}"
                return dict(entry), message
        except ValueError as exc:
            return None, str(exc)

    def _start_from_watermark(self, entry: dict[str, Any]) -> str:
        """消费运行水位与断点数据快照，把重跑写入范围冻结为未完成分区。"""
        runtime = self._find_runtime(str(entry["原始运行编号"]))
        if runtime is None:
            raise ValueError(f"原始运行记录 {entry['原始运行编号']} 不存在，无法读取水位快照")
        snapshot = partition.summarize_watermark(int(runtime["id"]))
        if snapshot is None:
            # 历史运行没有分区水位时保留旧口径：按重跑区间整段覆盖，不阻断老流程。
            entry["status"] = "执行中"
            entry["pending"] = True
            entry["resume_from_partition"] = None
            entry["resume_partitions"] = []
            entry["watermark_snapshot"] = None
            return "原始运行无分区水位，按旧口径覆盖整个重跑区间"

        if snapshot["state"] == "成功":
            raise ValueError("原始运行分区已全部成功，没有需要恢复的断点分区")
        resume_partitions = [
            item["partition_no"]
            for item in snapshot["partitions"]
            if item["state"] != partition.RESULT_SUCCESS
        ]
        # 冻结快照：之后原始运行再迟到事件也不会改变本次重跑的写入范围。
        entry["status"] = "执行中"
        entry["pending"] = True
        entry["resume_from_partition"] = snapshot["breakpoint_partition"]
        entry["resume_partitions"] = resume_partitions
        entry["watermark_version"] = snapshot["version"]
        entry["watermark_snapshot"] = snapshot
        entry["重跑区间开始"] = snapshot["breakpoint_partition"] or entry["重跑区间开始"]
        entry["重跑区间结束"] = (
            resume_partitions[-1] if resume_partitions else entry.get("重跑区间结束")
        )
        return (
            f"已按运行水位从断点分区 {snapshot['breakpoint_partition']} 恢复，"
            f"仅重跑未完成分区 {resume_partitions}"
        )

    def report_partition(
        self, entry_id: int, payload: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        """上报重跑分区进度，事务内落库事件、累计行数并按恢复范围判定完成状态。"""
        try:
            with store.transaction():
                entry = store.find(MODULE, entry_id)
                if entry is None:
                    return None, f"重跑任务 {entry_id} 不存在或已归档"
                if entry.get("status") != "执行中":
                    return None, "只有执行中的重跑任务才能上报分区进度"
                # ValueError 穿透 with 触发整事务回滚，不会留下半截重跑进度。
                event, outcome = partition.receive_rerun_event(entry, payload, _now())
                progress = partition.summarize_rerun(entry)

                entry["读取行数"] = progress["read_rows"]
                entry["写入行数"] = progress["write_rows"]
                if progress["finished"]:
                    entry["status"] = "已完成"
                    entry["pending"] = False
                message = {
                    partition.OUTCOME_APPLIED: "重跑分区进度已生效，断点恢复范围已推进",
                    partition.OUTCOME_DUPLICATE: "同请求重跑分区进度已接收过，本次幂等忽略",
                    partition.OUTCOME_CONFLICT: f"迟到旧重跑事件已拒绝：{event.get('reject_reason')}",
                }[outcome]
                return {
                    "outcome": outcome,
                    "message": message,
                    "event": event,
                    "progress": progress,
                    "entry": dict(entry),
                }, message
        except ValueError as exc:
            return None, str(exc)

    def get_progress(self, entry_id: int) -> dict[str, Any] | None:
        with store.locked():
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None
            if not entry.get("resume_partitions"):
                return {
                    "rerun_id": entry_id,
                    "resume_partitions": [],
                    "note": "该重跑任务未消费分区水位（旧口径整段重跑或尚未启动）",
                }
            return partition.summarize_rerun(entry)

    def list_partition_events(self, entry_id: int) -> list[dict[str, Any]] | None:
        with store.locked():
            if store.find(MODULE, entry_id) is None:
                return None
            return partition.list_rerun_events(entry_id)

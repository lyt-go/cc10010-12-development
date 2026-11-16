"""分区级运行水位与断点交接的端到端验收测试。

覆盖需求点：
1. 分区进度事件汇总运行水位与断点数据快照；
2. 同请求重复提交只生效一次（幂等）；
3. 跨进程迟到旧事件与当前水位冲突时被拒绝、留痕且水位不回退；
4. 水位与读取、写入行数、执行状态同一事务落库，异常整体回滚；
5. 进程断线重连从最后稳定分区继续；
6. 异常重跑消费快照，只从中断分区恢复且行数可回查；
7. 刷新/重读运行记录时执行状态与水位一致、原断点和行数保留。
"""
from __future__ import annotations

import threading
import unittest

from app.services import partition
from app.services.rerun import RerunService
from app.services.runtime import RuntimeService
from app.store import store

_RECORD_SEQ = 0


def fresh_runtime(service: RuntimeService, prefix: str = "RUNT-T") -> int:
    # 全局 store 跨用例共享，编号必须唯一，避免按编号查找命中别的用例数据。
    global _RECORD_SEQ
    _RECORD_SEQ += 1
    record_no = f"{prefix}-{_RECORD_SEQ:04d}"
    entry, missing = service.create_entry({
        "记录编号": record_no,
        "作业名称": "水位验收作业",
        "开始时间": "2026-11-16 10:00:00",
    })
    assert not missing
    return int(entry["id"])


class WatermarkLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        # 每个用例只操作自己新建的运行记录，互不干扰种子数据。
        self.runtime = RuntimeService()
        self.rerun = RerunService()
        self.run_id = fresh_runtime(self.runtime)

    def _report(self, no: str, seq: int, result: str, request_id: str,
                read: int = 100, write: int = 90, total: int | None = 4):
        payload = {
            "partition_no": no, "seq": seq, "result": result,
            "request_id": request_id, "read_rows": read, "write_rows": write,
        }
        if total is not None:
            payload["partition_total"] = total
        return self.runtime.report_partition(self.run_id, payload)

    def test_watermark_breakpoint_and_rows_accumulation(self) -> None:
        r1, _ = self._report("p001", 1, "成功", "req-p1", 120, 118)
        self.assertEqual(r1["outcome"], partition.OUTCOME_APPLIED)
        r2, _ = self._report("p002", 1, "成功", "req-p2", 130, 128, total=None)
        self.assertEqual(r2["outcome"], partition.OUTCOME_APPLIED)

        snap = self.runtime.get_watermark(self.run_id)
        self.assertEqual(snap["last_stable_partition"], "p002")
        self.assertEqual(snap["breakpoint_partition"], "p003")
        self.assertEqual(snap["read_rows"], 250)
        self.assertEqual(snap["write_rows"], 246)
        self.assertEqual(snap["state"], "运行中")

        # 失败分区出现：断点停在失败分区，执行状态同步为失败。
        r3, _ = self._report("p003", 1, "失败", "req-p3", 40, 0, total=None)
        self.assertEqual(r3["outcome"], partition.OUTCOME_APPLIED)
        snap = self.runtime.get_watermark(self.run_id)
        self.assertEqual(snap["breakpoint_partition"], "p003")
        self.assertEqual(snap["state"], "失败")
        self.assertEqual(snap["read_rows"], 290)

        entry = self.runtime.get_entry(self.run_id)
        self.assertEqual(entry["执行状态"], "失败")
        self.assertEqual(entry["status"], "失败")
        self.assertTrue(entry["abnormal"])
        self.assertEqual(entry["读取行数"], 290)
        self.assertEqual(entry["写入行数"], 246)
        self.assertEqual(entry["最后稳定分区"], "p002")
        self.assertEqual(entry["断点分区"], "p003")
        self.assertEqual(entry["分区完成数"], "2/4")

    def test_duplicate_request_applies_once(self) -> None:
        self._report("p001", 1, "成功", "dup-req", 100, 90)
        again, _ = self._report("p001", 1, "成功", "dup-req", 100, 90, total=None)
        self.assertEqual(again["outcome"], partition.OUTCOME_DUPLICATE)

        snap = self.runtime.get_watermark(self.run_id)
        # 重复提交不重复累计行数。
        self.assertEqual(snap["read_rows"], 100)
        self.assertEqual(snap["write_rows"], 90)
        events = self.runtime.list_partition_events(self.run_id)
        self.assertEqual(len(events), 1)

    def test_concurrent_duplicate_requests_only_one_effective(self) -> None:
        outcomes: list[str] = []

        def worker() -> None:
            result, _ = self._report("p001", 1, "成功", "concurrent-req", 100, 90)
            outcomes.append(result["outcome"])

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(outcomes.count(partition.OUTCOME_APPLIED), 1)
        self.assertEqual(outcomes.count(partition.OUTCOME_DUPLICATE), 7)
        snap = self.runtime.get_watermark(self.run_id)
        self.assertEqual(snap["read_rows"], 100)

    def test_late_old_event_conflicts_and_keeps_history(self) -> None:
        self._report("p001", 2, "成功", "req-p1-v2", 100, 90)
        # 旧进程迟到：seq=1 小于当前水位 seq=2。
        late, _ = self._report("p001", 1, "成功", "req-p1-late", 555, 555, total=None)
        self.assertEqual(late["outcome"], partition.OUTCOME_CONFLICT)
        self.assertFalse(late["event"]["effective"])
        self.assertIn("迟到", late["event"]["reject_reason"])

        snap = self.runtime.get_watermark(self.run_id)
        # 水位不回退、行数不被旧事件污染。
        self.assertEqual(snap["partitions"][0]["seq"], 2)
        self.assertEqual(snap["read_rows"], 100)
        events = self.runtime.list_partition_events(self.run_id)
        self.assertEqual(len(events), 2)  # 历史运行记录全部保留
        self.assertTrue(any(not e["effective"] for e in events))

        # 同分区更新的 seq 推进后可以正常覆盖汇总。
        newer, _ = self._report("p001", 3, "成功", "req-p1-v3", 110, 100, total=None)
        self.assertEqual(newer["outcome"], partition.OUTCOME_APPLIED)
        snap = self.runtime.get_watermark(self.run_id)
        self.assertEqual(snap["read_rows"], 110)
        self.assertEqual(snap["partitions"][0]["seq"], 3)

    def test_transaction_rollback_on_invalid_event(self) -> None:
        self._report("p001", 1, "成功", "req-p1", 100, 90)
        before = self.runtime.get_watermark(self.run_id)
        events_before = len(self.runtime.list_partition_events(self.run_id))

        # 非法 result 触发 ValueError，事务整体回滚。
        result, message = self.runtime.report_partition(self.run_id, {
            "partition_no": "p002", "seq": 1, "result": "未知态",
            "request_id": "req-bad", "read_rows": 1, "write_rows": 1,
        })
        self.assertIsNone(result)
        self.assertIn("result", message)

        after = self.runtime.get_watermark(self.run_id)
        self.assertEqual(after["read_rows"], before["read_rows"])
        self.assertEqual(after["version"], before["version"])
        self.assertEqual(len(self.runtime.list_partition_events(self.run_id)), events_before)

    def test_reopen_process_resumes_from_last_stable_partition(self) -> None:
        self._report("p001", 1, "成功", "req-p1", 100, 90)
        self._report("p002", 1, "失败", "req-p2", 20, 0, total=None)

        result, message = self.runtime.reopen_process(self.run_id, {"process_id": "worker-B"})
        self.assertIsNotNone(result)
        self.assertEqual(result["session"]["last_stable_partition"], "p001")
        self.assertEqual(result["session"]["resume_from_partition"], "p002")
        self.assertIn("断点", message)

        sessions = partition.list_process_sessions(self.run_id)
        self.assertEqual(len(sessions), 1)

    def test_refresh_keeps_status_and_rows_consistent(self) -> None:
        self._report("p001", 1, "成功", "req-p1", 100, 90)
        self._report("p002", 1, "成功", "req-p2", 100, 90, total=None)

        first = self.runtime.get_entry(self.run_id)
        # 模拟页面刷新、返回、重新进入详情、再次读取列表。
        for _ in range(3):
            again = self.runtime.get_entry(self.run_id)
            self.assertEqual(again["执行状态"], first["执行状态"])
            self.assertEqual(again["读取行数"], first["读取行数"])
            self.assertEqual(again["写入行数"], first["写入行数"])
            self.assertEqual(again["断点分区"], first["断点分区"])
            self.assertEqual(again["水位版本"], first["水位版本"])
        items, total = self.runtime.list_entries(keyword=first["记录编号"])
        self.assertEqual(total, 1)
        self.assertEqual(items[0]["执行状态"], "运行中")
        self.assertEqual(items[0]["读取行数"], 200)
        self.assertEqual(items[0]["断点分区"], "p003")


class RerunResumeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = RuntimeService()
        self.rerun = RerunService()
        self.run_id = fresh_runtime(self.runtime, "RUNT-R")
        self.record_no = self.runtime.get_entry(self.run_id)["记录编号"]
        # 4 个分区：p001/p002 成功，p003 失败，p004 未跑。
        for no, seq, result, req, read, write in [
            ("p001", 1, "成功", "r1-p1", 120, 118),
            ("p002", 1, "成功", "r1-p2", 130, 128),
            ("p003", 1, "失败", "r1-p3", 40, 0),
        ]:
            payload = {"partition_no": no, "seq": seq, "result": result,
                       "request_id": req, "read_rows": read, "write_rows": write}
            if no == "p001":
                payload["partition_total"] = 4
            res, _ = self.runtime.report_partition(self.run_id, payload)
            self.assertEqual(res["outcome"], partition.OUTCOME_APPLIED)

        entry, missing = self.rerun.create_entry({
            "任务编号": "RERU-T1",
            "原始运行编号": self.record_no,
            "重跑区间开始": "p001",
            "重跑原因": "p003 失败后断点重跑",
        })
        assert not missing
        self.rerun_id = int(entry["id"])

    def test_rerun_starts_only_from_breakpoint_partitions(self) -> None:
        entry, message = self.rerun.run_action(self.rerun_id, "启动重跑")
        self.assertEqual(entry["status"], "执行中")
        self.assertEqual(entry["resume_from_partition"], "p003")
        # 已成功的 p001/p002 不在重跑写入范围。
        self.assertEqual(entry["resume_partitions"], ["p003", "p004"])
        self.assertEqual(entry["重跑区间开始"], "p003")
        self.assertEqual(entry["重跑区间结束"], "p004")
        self.assertIn("p003", message)
        # 冻结的快照保留原始断点与行数，可核对。
        self.assertEqual(entry["watermark_snapshot"]["read_rows"], 290)
        self.assertEqual(entry["watermark_snapshot"]["write_rows"], 246)

    def _start(self) -> None:
        entry, _ = self.rerun.run_action(self.rerun_id, "启动重跑")
        self.assertEqual(entry["status"], "执行中")

    def test_rerun_rejects_partition_outside_resume_range(self) -> None:
        self._start()
        result, message = self.rerun.report_partition(self.rerun_id, {
            "partition_no": "p001", "seq": 1, "result": "成功",
            "request_id": "rerun-p1", "read_rows": 999, "write_rows": 999,
        })
        self.assertIsNone(result)
        self.assertIn("不在断点恢复范围", message)

    def test_rerun_full_resume_with_rows_traceable(self) -> None:
        self._start()
        r3, _ = self.rerun.report_partition(self.rerun_id, {
            "partition_no": "p003", "seq": 1, "result": "成功",
            "request_id": "rerun-p3", "read_rows": 88, "write_rows": 85,
        })
        self.assertEqual(r3["outcome"], partition.OUTCOME_APPLIED)
        self.assertEqual(r3["entry"]["status"], "执行中")

        # 同请求重复提交只生效一次。
        dup, _ = self.rerun.report_partition(self.rerun_id, {
            "partition_no": "p003", "seq": 1, "result": "成功",
            "request_id": "rerun-p3", "read_rows": 88, "write_rows": 85,
        })
        self.assertEqual(dup["outcome"], partition.OUTCOME_DUPLICATE)

        # 迟到旧事件冲突拒绝并留痕。
        late, _ = self.rerun.report_partition(self.rerun_id, {
            "partition_no": "p003", "seq": 1, "result": "成功",
            "request_id": "rerun-p3-late", "read_rows": 1, "write_rows": 1,
        })
        self.assertEqual(late["outcome"], partition.OUTCOME_CONFLICT)

        r4, _ = self.rerun.report_partition(self.rerun_id, {
            "partition_no": "p004", "seq": 1, "result": "成功",
            "request_id": "rerun-p4", "read_rows": 77, "write_rows": 75,
        })
        self.assertEqual(r4["outcome"], partition.OUTCOME_APPLIED)
        self.assertEqual(r4["entry"]["status"], "已完成")
        self.assertFalse(r4["entry"]["pending"])

        progress = self.rerun.get_progress(self.rerun_id)
        self.assertTrue(progress["finished"])
        self.assertEqual(progress["read_rows"], 88 + 77)
        self.assertEqual(progress["write_rows"], 85 + 75)
        self.assertEqual(progress["resume_partitions"], ["p003", "p004"])

        # 行数可逐条事件回查。
        events = self.rerun.list_partition_events(self.rerun_id)
        effective = [e for e in events if e["effective"]]
        self.assertEqual(len(effective), 2)
        self.assertEqual(sum(e["read_rows"] for e in effective), 88 + 77)
        self.assertEqual(sum(e["write_rows"] for e in effective), 85 + 75)
        self.assertTrue(any(not e["effective"] for e in events))

        # 刷新列表后口径一致，原断点保留。
        entry = self.rerun.get_entry(self.rerun_id)
        self.assertEqual(entry["断点分区"], "p003")
        self.assertEqual(entry["恢复分区数"], "2/2")
        self.assertEqual(entry["读取行数"], 88 + 77)
        self.assertEqual(entry["写入行数"], 85 + 75)

    def test_late_runtime_event_after_rerun_start_does_not_move_range(self) -> None:
        self._start()
        entry = self.rerun.get_entry(self.rerun_id)
        frozen = list(entry["resume_partitions"])

        # 重跑启动后原始运行又收到迟到的新事件，重跑写入范围保持冻结。
        res, _ = self.runtime.report_partition(self.run_id, {
            "partition_no": "p003", "seq": 2, "result": "成功",
            "request_id": "runtime-p3-retry", "read_rows": 70, "write_rows": 66,
        })
        self.assertEqual(res["outcome"], partition.OUTCOME_APPLIED)
        entry = self.rerun.get_entry(self.rerun_id)
        self.assertEqual(entry["resume_partitions"], frozen)
        self.assertEqual(entry["watermark_snapshot"]["breakpoint_partition"], "p003")


class EdgeCaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = RuntimeService()
        self.rerun = RerunService()

    def test_rerun_without_watermark_keeps_legacy_full_range(self) -> None:
        run_id = fresh_runtime(self.runtime, "RUNT-LEGACY")
        record_no = self.runtime.get_entry(run_id)["记录编号"]
        entry, missing = self.rerun.create_entry({
            "任务编号": "RERU-LEGACY", "原始运行编号": record_no,
            "重跑区间开始": "p001",
        })
        assert not missing
        rerun_id = int(entry["id"])
        entry, message = self.rerun.run_action(rerun_id, "启动重跑")
        self.assertEqual(entry["status"], "执行中")
        self.assertEqual(entry["resume_partitions"], [])
        self.assertIn("旧口径", message)
        # 无恢复范围时不允许上报分区事件。
        result, _ = self.rerun.report_partition(rerun_id, {
            "partition_no": "p001", "seq": 1, "result": "成功",
            "request_id": "x", "read_rows": 1, "write_rows": 1,
        })
        self.assertIsNone(result)

    def test_rerun_start_rejected_when_all_partitions_succeeded(self) -> None:
        run_id = fresh_runtime(self.runtime, "RUNT-OK")
        record_no = self.runtime.get_entry(run_id)["记录编号"]
        for idx, req in enumerate(["ok-1", "ok-2"], start=1):
            payload = {"partition_no": f"p00{idx}", "seq": 1, "result": "成功",
                       "request_id": req, "read_rows": 10, "write_rows": 9}
            if idx == 1:
                payload["partition_total"] = 2
            result, _ = self.runtime.report_partition(run_id, payload)
            self.assertEqual(result["outcome"], partition.OUTCOME_APPLIED)
        entry, _ = self.rerun.create_entry({
            "任务编号": "RERU-OK", "原始运行编号": record_no, "重跑区间开始": "p001",
        })
        result, message = self.rerun.run_action(int(entry["id"]), "启动重跑")
        self.assertIsNone(result)
        self.assertIn("没有需要恢复的断点", message)

    def test_watermark_404_and_event_validation(self) -> None:
        run_id = fresh_runtime(self.runtime, "RUNT-VAL")
        self.assertIsNone(self.runtime.get_watermark(run_id))
        self.assertIsNone(self.runtime.get_watermark(999999))

        # 缺 partition_total 的首次上报。
        result, message = self.runtime.report_partition(run_id, {
            "partition_no": "p001", "seq": 1, "result": "成功",
            "request_id": "v1", "read_rows": 1, "write_rows": 1,
        })
        self.assertIsNone(result)
        self.assertIn("partition_total", message)

        # 非法分区编号、负数行数、缺 request_id。
        for bad in [
            {"partition_no": "x1", "seq": 1, "result": "成功", "request_id": "v",
             "read_rows": 1, "write_rows": 1, "partition_total": 3},
            {"partition_no": "p001", "seq": 1, "result": "成功", "request_id": "v",
             "read_rows": -1, "write_rows": 1, "partition_total": 3},
            {"partition_no": "p001", "seq": 1, "result": "成功", "request_id": "",
             "read_rows": 1, "write_rows": 1, "partition_total": 3},
            {"partition_no": "p009", "seq": 1, "result": "成功", "request_id": "v",
             "read_rows": 1, "write_rows": 1, "partition_total": 3},
        ]:
            result, message = self.runtime.report_partition(run_id, bad)
            self.assertIsNone(result, msg=message)
            self.assertTrue(message)
        # 全部非法事件都已回滚，未产生任何水位。
        self.assertIsNone(self.runtime.get_watermark(run_id))

    def test_rerun_report_only_when_running(self) -> None:
        run_id = fresh_runtime(self.runtime, "RUNT-STATE")
        record_no = self.runtime.get_entry(run_id)["记录编号"]
        self.runtime.report_partition(run_id, {
            "partition_no": "p001", "seq": 1, "result": "失败",
            "request_id": "s1", "read_rows": 1, "write_rows": 0, "partition_total": 2,
        })
        entry, _ = self.rerun.create_entry({
            "任务编号": "RERU-STATE", "原始运行编号": record_no, "重跑区间开始": "p001",
        })
        rerun_id = int(entry["id"])
        # 草稿态不能上报。
        result, message = self.rerun.report_partition(rerun_id, {
            "partition_no": "p001", "seq": 1, "result": "成功",
            "request_id": "z", "read_rows": 1, "write_rows": 1,
        })
        self.assertIsNone(result)
        self.assertIn("执行中", message)


class SeedDataTest(unittest.TestCase):
    def test_seed_runtime_watermark_visible_on_list(self) -> None:
        service = RuntimeService()
        entry = service.get_entry(2)  # RUNT-0002
        self.assertEqual(entry["执行状态"], "失败")
        self.assertEqual(entry["最后稳定分区"], "p004")
        self.assertEqual(entry["断点分区"], "p005")
        self.assertEqual(entry["读取行数"], 563)
        self.assertEqual(entry["写入行数"], 495)
        self.assertEqual(entry["分区完成数"], "4/6")

        snap = service.get_watermark(2)
        self.assertEqual(snap["partition_total"], 6)
        events = service.list_partition_events(2)
        self.assertTrue(any(not e["effective"] for e in events))


if __name__ == "__main__":
    unittest.main(verbosity=2)

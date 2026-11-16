"""真实 HTTP 链路冒烟：起 uvicorn 后用 urllib 走完整的水位与重跑流程。

只依赖标准库，避免额外安装依赖。执行：
    .venv/bin/python tests/http_smoke.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://127.0.0.1:8011"
BACKEND = Path(__file__).resolve().parent.parent


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def expect(cond: bool, message: str) -> None:
    if not cond:
        raise AssertionError(message)
    print(f"  ✓ {message}")


def main() -> None:
    proc = subprocess.Popen(
        [".venv/bin/uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8011"],
        cwd=BACKEND, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(50):
            try:
                call("GET", "/api/health")
                break
            except OSError:
                time.sleep(0.2)

        print("1) 种子运行记录的水位与断点")
        status, payload = call("GET", "/api/runtime/2/watermark")
        expect(status == 200, "GET /api/runtime/2/watermark 返回 200")
        expect(payload["breakpoint_partition"] == "p005", "RUNT-0002 断点分区为 p005")
        expect(payload["last_stable_partition"] == "p004", "最后稳定分区为 p004")
        expect(payload["read_rows"] == 563 and payload["write_rows"] == 495,
               "累计读取 563 / 写入 495 可回查")
        status, payload = call("GET", "/api/runtime/2/partition-events")
        expect(any(not e["effective"] for e in payload["items"]), "迟到事件留痕且标记无效")

        print("2) 新建运行并上报分区进度")
        status, payload = call("POST", "/api/runtime", {"values": {
            "记录编号": "RUNT-HTTP1", "作业名称": "HTTP冒烟作业", "开始时间": "2026-11-16",
        }})
        run_id = payload["entry"]["id"]
        for no, result, req, read, write, seq in [
            ("p001", "成功", "http-p1", 100, 90, 1),
            ("p002", "成功", "http-p2", 110, 100, 1),
            ("p003", "失败", "http-p3", 30, 0, 1),
        ]:
            values = {"partition_no": no, "result": result, "request_id": req,
                      "read_rows": read, "write_rows": write, "seq": seq}
            if no == "p001":
                values["partition_total"] = 4
            status, payload = call("POST", f"/api/runtime/{run_id}/partition-events",
                                   {"values": values})
            expect(status == 200 and payload["entry"]["outcome"] == "applied",
                   f"{no} 事件生效")

        print("3) 重复请求幂等 + 迟到旧事件冲突")
        status, payload = call("POST", f"/api/runtime/{run_id}/partition-events", {"values": {
            "partition_no": "p001", "result": "成功", "request_id": "http-p1",
            "read_rows": 100, "write_rows": 90, "seq": 1,
        }})
        expect(payload["entry"]["outcome"] == "duplicate", "同 request_id 重复提交幂等忽略")
        status, payload = call("POST", f"/api/runtime/{run_id}/partition-events", {"values": {
            "partition_no": "p001", "result": "成功", "request_id": "http-p1-late",
            "read_rows": 1, "write_rows": 1, "seq": 1,
        }})
        expect(payload["entry"]["outcome"] == "conflict", "迟到旧分区事件冲突拒绝")
        status, payload = call("GET", f"/api/runtime/{run_id}/watermark")
        expect(payload["read_rows"] == 240 and payload["write_rows"] == 190,
               "水位行数未被重复/迟到事件污染")

        print("4) 进程断线重连接断点")
        status, payload = call("POST", f"/api/runtime/{run_id}/process-sessions",
                               {"values": {"process_id": "worker-http-2"}})
        expect(status == 200, "重连接口返回 200")
        expect(payload["entry"]["session"]["resume_from_partition"] == "p003",
               "新进程从断点 p003 继续")

        print("5) 异常重跑只恢复未完成分区")
        status, payload = call("POST", "/api/rerun", {"values": {
            "任务编号": "RERU-HTTP1", "原始运行编号": "RUNT-HTTP1",
            "重跑区间开始": "p001", "重跑原因": "HTTP 冒烟断点重跑",
        }})
        rerun_id = payload["entry"]["id"]
        status, payload = call("POST", f"/api/rerun/{rerun_id}/actions",
                               {"values": {"action": "启动重跑"}})
        expect(status == 200, "启动重跑成功")
        expect(payload["entry"]["resume_partitions"] == ["p003", "p004"],
               "重跑范围冻结为未完成分区 p003/p004")

        status, payload = call("POST", f"/api/rerun/{rerun_id}/partition-events", {"values": {
            "partition_no": "p001", "result": "成功", "request_id": "bad-range",
            "read_rows": 1, "write_rows": 1, "seq": 1,
        }})
        expect(status == 200 and payload["ok"] is False, "范围外分区 p001 被拒绝")

        for no, req, read, write in [("p003", "rr-p3", 66, 60), ("p004", "rr-p4", 55, 50)]:
            status, payload = call("POST", f"/api/rerun/{rerun_id}/partition-events",
                                   {"values": {"partition_no": no, "result": "成功",
                                               "request_id": req, "read_rows": read,
                                               "write_rows": write, "seq": 1}})
            expect(payload["entry"]["outcome"] == "applied", f"重跑 {no} 生效")
        status, payload = call("GET", f"/api/rerun/{rerun_id}/progress")
        expect(payload["finished"] and payload["read_rows"] == 121
               and payload["write_rows"] == 110, "重跑完成且读取 121 / 写入 110 可回查")
        status, payload = call("GET", f"/api/rerun/{rerun_id}")
        expect(payload["status"] == "已完成" and payload["断点分区"] == "p003",
               "重跑记录状态与原断点保留一致")

        print("6) 刷新重读运行记录，状态与水位一致")
        status, payload = call("GET", f"/api/runtime/{run_id}")
        expect(payload["执行状态"] == "失败" and payload["断点分区"] == "p003"
               and payload["读取行数"] == 240, "运行记录执行状态、断点、行数与水位同步")

        print("\n全部 HTTP 冒烟断言通过。")
    finally:
        proc.terminate()
        proc.wait(timeout=10)


if __name__ == "__main__":
    sys.exit(main())

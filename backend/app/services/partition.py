"""分区级运行水位与断点交接的核心规则。

同一条运行实例（或重跑任务）的分区进度可能：

- 由同一请求重复提交（网络重试）：按 request_id 幂等，只生效一次；
- 跨进程迟到（旧进程断线后又把旧分区结果送回来）：按分区单调序号 seq 判定，
  旧序号与当前水位冲突时拒绝，水位绝不回退；
- 正常推进：更新分区明细、重算最后稳定分区/断点与累计读取、写入行数。

所有状态变更都在调用方开启的 ``store.transaction()`` 内完成，事件表只追加、
不修改不删除，保证历史运行记录可核对。
"""
from __future__ import annotations

from typing import Any

from app.store import store

RUNTIME_EVENT_TABLE = "runtime_partition_event"
RUNTIME_WATERMARK_TABLE = "runtime_watermark"
PROCESS_SESSION_TABLE = "runtime_process_session"
RERUN_EVENT_TABLE = "rerun_partition_event"

# 事件处理结论：applied=本次生效；duplicate=重复请求，幂等忽略；conflict=迟到旧事件，冲突拒绝。
OUTCOME_APPLIED = "applied"
OUTCOME_DUPLICATE = "duplicate"
OUTCOME_CONFLICT = "conflict"

RESULT_SUCCESS = "成功"
RESULT_FAILED = "失败"
VALID_RESULTS = (RESULT_SUCCESS, RESULT_FAILED)


def partition_index(partition_no: str) -> int:
    """把分区编号（如 p003）转成从 1 开始的有序序号；无法识别时按 0 处理。"""
    text = str(partition_no or "").strip().lower()
    if text.startswith("p") and text[1:].isdigit():
        return int(text[1:])
    return 0


def partition_no(index: int) -> str:
    return f"p{int(index):03d}"


def partition_range(total: int) -> list[str]:
    return [partition_no(i) for i in range(1, int(total) + 1)]


def _to_int(value: Any, field: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}必须是非负整数，收到：{value!r}") from exc
    if number < 0:
        raise ValueError(f"{field}不能为负数，收到：{number}")
    return number


def _find_events(table: str, run_id: int) -> list[dict[str, Any]]:
    return [
        row
        for row in store.rows(table)
        if int(row.get("run_id", 0)) == int(run_id)
        and row.get("effective") is not False
    ]


def _all_event_rows(table: str, run_id: int) -> list[dict[str, Any]]:
    return [row for row in store.rows(table) if int(row.get("run_id", 0)) == int(run_id)]


def find_watermark(run_id: int) -> dict[str, Any] | None:
    for row in store.rows(RUNTIME_WATERMARK_TABLE):
        if int(row.get("run_id", 0)) == int(run_id):
            return row
    return None


def list_events(run_id: int) -> list[dict[str, Any]]:
    """返回运行实例的全部分区进度事件（含被拒绝的迟到事件），历史记录不丢。"""
    return _all_event_rows(RUNTIME_EVENT_TABLE, run_id)


def summarize_watermark(run_id: int) -> dict[str, Any] | None:
    """从已生效事件重算水位与断点数据快照。

    最后稳定分区 = 从 p001 起连续成功的最长前缀的末位；
    断点分区 = 第一个未成功的分区（失败或尚未上报），全部成功则为空。
    """
    watermark = find_watermark(run_id)
    if watermark is None:
        return None
    total = int(watermark["partition_total"])
    events = sorted(_find_events(RUNTIME_EVENT_TABLE, run_id), key=lambda row: row["id"])

    details: dict[str, dict[str, Any]] = {}
    read_rows = write_rows = 0
    success_indexes: set[int] = set()
    failed_indexes: set[int] = set()
    for event in events:
        idx = partition_index(event["partition_no"])
        details[event["partition_no"]] = {
            "partition_no": event["partition_no"],
            "partition_index": idx,
            "result": event["result"],
            "seq": event["seq"],
            "read_rows": int(event["read_rows"]),
            "write_rows": int(event["write_rows"]),
            "updated_at": event["created_at"],
            "request_id": event["request_id"],
        }
        read_rows += int(event["read_rows"])
        write_rows += int(event["write_rows"])
        if event["result"] == RESULT_SUCCESS:
            success_indexes.add(idx)
        else:
            failed_indexes.add(idx)

    partitions: list[dict[str, Any]] = []
    last_stable_index = 0
    breakpoint_index: int | None = None
    for idx in range(1, total + 1):
        no = partition_no(idx)
        item = details.get(no)
        if item is None:
            partitions.append({
                "partition_no": no,
                "partition_index": idx,
                "state": "待处理",
                "seq": None,
                "read_rows": 0,
                "write_rows": 0,
            })
        else:
            partitions.append({
                "partition_no": no,
                "partition_index": idx,
                "state": item["result"],
                "seq": item["seq"],
                "read_rows": item["read_rows"],
                "write_rows": item["write_rows"],
            })
        if breakpoint_index is None and idx not in success_indexes:
            breakpoint_index = idx
        if breakpoint_index is None:
            last_stable_index = idx

    last_stable_partition = partition_no(last_stable_index) if last_stable_index else None
    breakpoint_partition = partition_no(breakpoint_index) if breakpoint_index else None
    finished = len(success_indexes) == total and not failed_indexes
    if finished:
        state = "成功"
    elif failed_indexes:
        state = "失败"
    else:
        state = "运行中"

    return {
        "run_id": int(run_id),
        "record_no": watermark.get("record_no"),
        "partition_total": total,
        "last_stable_partition": last_stable_partition,
        "breakpoint_partition": breakpoint_partition,
        "success_partition_count": len(success_indexes),
        "failed_partition_count": len(failed_indexes),
        "read_rows": read_rows,
        "write_rows": write_rows,
        "state": state,
        "partitions": partitions,
        "updated_at": watermark.get("updated_at"),
        "version": int(watermark.get("version", 0)),
    }


def _upsert_watermark(run_id: int, record_no: str, total: int, now: str) -> dict[str, Any]:
    watermark = find_watermark(run_id)
    if watermark is None:
        watermark = {
            "id": store.next_id(RUNTIME_WATERMARK_TABLE),
            "run_id": int(run_id),
            "record_no": record_no,
            "partition_total": int(total),
            "version": 0,
            "updated_at": now,
        }
        store.rows(RUNTIME_WATERMARK_TABLE).append(watermark)
    return watermark


def receive_runtime_event(run_id: int, payload: dict[str, Any], now: str) -> tuple[dict[str, Any], str]:
    """接收一条运行分区进度事件，返回 (事件记录, 处理结论)。

    必须在 ``store.transaction()`` 内调用。冲突/重复事件也会留痕（effective=False），
    但不会改动水位与行数，旧有历史运行记录保持原样。
    """
    partition_no_value = str(payload.get("partition_no") or "").strip()
    if not partition_no_value:
        raise ValueError("partition_no 不能为空")
    idx = partition_index(partition_no_value)
    if idx <= 0:
        raise ValueError(f"分区编号格式应为 p001、p002 这类形式，收到：{partition_no_value}")

    seq = _to_int(payload.get("seq", 0), "seq")
    read_rows = _to_int(payload.get("read_rows", 0), "read_rows")
    write_rows = _to_int(payload.get("write_rows", 0), "write_rows")
    result = str(payload.get("result") or "").strip()
    if result not in VALID_RESULTS:
        raise ValueError(f"result 只支持 {VALID_RESULTS}，收到：{result!r}")
    request_id = str(payload.get("request_id") or "").strip()
    if not request_id:
        raise ValueError("request_id 不能为空，用于同请求重复提交的幂等判定")

    table_rows = store.rows(RUNTIME_EVENT_TABLE)
    total = payload.get("partition_total")
    existing = find_watermark(run_id)
    if total is None and existing is None:
        raise ValueError("首次上报分区进度时必须带 partition_total 声明总分区数")
    if total is not None:
        total = _to_int(total, "partition_total")
        if total <= 0:
            raise ValueError("partition_total 必须大于 0")
        if existing is not None and int(existing["partition_total"]) != total:
            raise ValueError(
                f"分区总数已固定为 {existing['partition_total']}，不允许改成 {total}"
            )
    else:
        total = int(existing["partition_total"])  # type: ignore[index]
    if idx > total:
        raise ValueError(f"分区 {partition_no_value} 超出总分区数 {total}")

    # 同请求重复提交：request_id 命中即幂等返回，事件与水位都不再变动。
    duplicate = next(
        (row for row in table_rows if int(row.get("run_id", 0)) == int(run_id)
         and str(row.get("request_id")) == request_id),
        None,
    )
    if duplicate is not None:
        return duplicate, OUTCOME_DUPLICATE

    # 跨进程迟到：同分区只接受更大的 seq，旧结果与当前水位冲突时拒绝且不回退水位。
    applied = _find_events(RUNTIME_EVENT_TABLE, run_id)
    current = next(
        (row for row in applied if str(row.get("partition_no")) == partition_no_value),
        None,
    )
    if current is not None and seq <= int(current["seq"]):
        rejected = {
            "id": store.next_id(RUNTIME_EVENT_TABLE),
            "run_id": int(run_id),
            "partition_no": partition_no_value,
            "partition_index": idx,
            "seq": seq,
            "result": result,
            "read_rows": read_rows,
            "write_rows": write_rows,
            "request_id": request_id,
            "effective": False,
            "reject_reason": (
                f"迟到旧事件 seq={seq} 不大于当前水位 seq={current['seq']}，"
                "按分区单调规则拒绝，水位保持不变"
            ),
            "created_at": now,
        }
        table_rows.append(rejected)
        return rejected, OUTCOME_CONFLICT

    event = {
        "id": store.next_id(RUNTIME_EVENT_TABLE),
        "run_id": int(run_id),
        "partition_no": partition_no_value,
        "partition_index": idx,
        "seq": seq,
        "result": result,
        "read_rows": read_rows,
        "write_rows": write_rows,
        "request_id": request_id,
        "effective": True,
        "reject_reason": None,
        "created_at": now,
    }
    table_rows.append(event)
    # 同分区更高 seq 的结果生效后，旧事件保留为历史记录，但不再计入水位与行数，
    # 避免同一分区被新旧两条事件重复累计。
    if current is not None:
        current["effective"] = False
        current["reject_reason"] = (
            f"已被 seq={seq} 的新分区事件取代，历史保留但不再计入运行水位"
        )

    record_no = payload.get("record_no") or (existing.get("record_no") if existing else None)
    watermark = _upsert_watermark(run_id, str(record_no), total, now)  # type: ignore[arg-type]
    watermark["version"] = int(watermark.get("version", 0)) + 1
    watermark["updated_at"] = now
    return event, OUTCOME_APPLIED


def open_process_session(run_id: int, process_id: str, now: str) -> dict[str, Any]:
    """进程断线后重新开始：登记会话并交接最后稳定分区，新进程从下一分区继续。"""
    watermark = summarize_watermark(run_id)
    if watermark is None:
        raise ValueError(f"运行记录 {run_id} 还没有分区水位，无法交接断点")
    session = {
        "id": store.next_id(PROCESS_SESSION_TABLE),
        "run_id": int(run_id),
        "process_id": str(process_id),
        "last_stable_partition": watermark["last_stable_partition"],
        "resume_from_partition": watermark["breakpoint_partition"],
        "watermark_version": watermark["version"],
        "created_at": now,
    }
    store.rows(PROCESS_SESSION_TABLE).append(session)
    return session


def list_process_sessions(run_id: int) -> list[dict[str, Any]]:
    return [
        row for row in store.rows(PROCESS_SESSION_TABLE)
        if int(row.get("run_id", 0)) == int(run_id)
    ]


# ----------------------------------------------------------------------
# 异常重跑侧：消费运行水位快照，只恢复未完成分区
# ----------------------------------------------------------------------

def find_rerun_events(rerun_id: int) -> list[dict[str, Any]]:
    return [
        row for row in store.rows(RERUN_EVENT_TABLE)
        if int(row.get("rerun_id", 0)) == int(rerun_id)
        and row.get("effective") is not False
    ]


def list_rerun_events(rerun_id: int) -> list[dict[str, Any]]:
    """重跑分区事件全量留痕，包含被幂等忽略和被冲突拒绝的记录。"""
    return [
        row for row in store.rows(RERUN_EVENT_TABLE)
        if int(row.get("rerun_id", 0)) == int(rerun_id)
    ]


def summarize_rerun(rerun: dict[str, Any]) -> dict[str, Any]:
    """汇总重跑任务的恢复分区进度与累计读取、写入行数。"""
    rerun_id = int(rerun["id"])
    resume_partitions = list(rerun.get("resume_partitions") or [])
    events = sorted(find_rerun_events(rerun_id), key=lambda row: row["id"])

    done: set[str] = set()
    read_rows = write_rows = 0
    per_partition: dict[str, dict[str, Any]] = {}
    for event in events:
        no = str(event["partition_no"])
        done.add(no)
        read_rows += int(event["read_rows"])
        write_rows += int(event["write_rows"])
        per_partition[no] = {
            "partition_no": no,
            "state": event["result"],
            "seq": event["seq"],
            "read_rows": int(event["read_rows"]),
            "write_rows": int(event["write_rows"]),
        }

    partitions = []
    for no in resume_partitions:
        item = per_partition.get(no)
        partitions.append({
            "partition_no": no,
            "state": item["state"] if item else "待处理",
            "read_rows": item["read_rows"] if item else 0,
            "write_rows": item["write_rows"] if item else 0,
        })
    finished = bool(resume_partitions) and all(no in done for no in resume_partitions)
    return {
        "rerun_id": rerun_id,
        "resume_from_partition": rerun.get("resume_from_partition"),
        "resume_partitions": resume_partitions,
        "resume_partition_total": len(resume_partitions),
        "finished_partition_count": len(done),
        "read_rows": read_rows,
        "write_rows": write_rows,
        "finished": finished,
        "partitions": partitions,
    }


def receive_rerun_event(
    rerun: dict[str, Any], payload: dict[str, Any], now: str
) -> tuple[dict[str, Any], str]:
    """接收重跑分区进度事件，只允许写入快照圈定的未完成分区，同样幂等、防迟到。"""
    rerun_id = int(rerun["id"])
    partition_no_value = str(payload.get("partition_no") or "").strip()
    if not partition_no_value:
        raise ValueError("partition_no 不能为空")
    seq = _to_int(payload.get("seq", 0), "seq")
    read_rows = _to_int(payload.get("read_rows", 0), "read_rows")
    write_rows = _to_int(payload.get("write_rows", 0), "write_rows")
    result = str(payload.get("result") or "").strip()
    if result not in VALID_RESULTS:
        raise ValueError(f"result 只支持 {VALID_RESULTS}，收到：{result!r}")
    request_id = str(payload.get("request_id") or "").strip()
    if not request_id:
        raise ValueError("request_id 不能为空，用于同请求重复提交的幂等判定")

    resume_partitions = set(rerun.get("resume_partitions") or [])
    if not resume_partitions:
        raise ValueError("该重跑任务没有可恢复的未完成分区")
    if partition_no_value not in resume_partitions:
        raise ValueError(
            f"分区 {partition_no_value} 不在断点恢复范围 "
            f"{sorted(resume_partitions)} 内，重跑只从中断分区继续"
        )

    table_rows = store.rows(RERUN_EVENT_TABLE)
    duplicate = next(
        (row for row in table_rows if int(row.get("rerun_id", 0)) == rerun_id
         and str(row.get("request_id")) == request_id),
        None,
    )
    if duplicate is not None:
        return duplicate, OUTCOME_DUPLICATE

    applied = find_rerun_events(rerun_id)
    current = next(
        (row for row in applied if str(row.get("partition_no")) == partition_no_value),
        None,
    )
    if current is not None and seq <= int(current["seq"]):
        rejected = {
            "id": store.next_id(RERUN_EVENT_TABLE),
            "rerun_id": rerun_id,
            "partition_no": partition_no_value,
            "seq": seq,
            "result": result,
            "read_rows": read_rows,
            "write_rows": write_rows,
            "request_id": request_id,
            "effective": False,
            "reject_reason": (
                f"迟到旧事件 seq={seq} 不大于当前重跑进度 seq={current['seq']}，"
                "按分区单调规则拒绝"
            ),
            "created_at": now,
        }
        table_rows.append(rejected)
        return rejected, OUTCOME_CONFLICT

    event = {
        "id": store.next_id(RERUN_EVENT_TABLE),
        "rerun_id": rerun_id,
        "partition_no": partition_no_value,
        "seq": seq,
        "result": result,
        "read_rows": read_rows,
        "write_rows": write_rows,
        "request_id": request_id,
        "effective": True,
        "reject_reason": None,
        "created_at": now,
    }
    table_rows.append(event)
    if current is not None:
        current["effective"] = False
        current["reject_reason"] = (
            f"已被 seq={seq} 的新重跑事件取代，历史保留但不再累计行数"
        )
    return event, OUTCOME_APPLIED

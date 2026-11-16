"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

分区级水位与断点能力依赖三张内部表：
- runtime_partition_event：运行分区进度事件（追加写，历史运行记录全部保留）
- runtime_watermark：每个运行实例一份的运行水位与断点数据快照
- rerun_partition_event：异常重跑分区进度事件

所有写动作都走 ``store.transaction()``：进入时加锁并深拷贝整库，业务函数只在
提交点一次性改数据，抛异常即回滚，用来模拟「水位与读取、写入行数同一事务落库」。
"""
from __future__ import annotations

import copy
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from app.seed import SEED_ROWS

# 分区水位相关的内部表：不进运营概览计数，但参与事务快照与回滚。
PARTITION_TABLES = (
    "runtime_partition_event",
    "runtime_watermark",
    "runtime_process_session",
    "rerun_partition_event",
)


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        for name in PARTITION_TABLES:
            self._tables.setdefault(name, [])
        # 跨进程迟到事件、并发请求都会争用水位，所有写操作在同一把锁内完成。
        self._lock = threading.RLock()

    def module_names(self) -> list[str]:
        return sorted(name for name in self._tables if name not in PARTITION_TABLES)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_id(self, module: str) -> int:
        return max((int(row.get("id", 0)) for row in self.rows(module)), default=0) + 1

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """模拟数据库本地事务：加锁、提交点写入、异常整体回滚。

        业务函数在 ``with store.transaction():`` 内对表做的全部修改，要么随 with
        正常结束一起生效，要么在抛异常时靠进入时的深拷贝快照整体还原，保证
        「水位、断点、读取/写入行数、执行状态」不会出现只落一半的中间态。
        """
        with self._lock:
            snapshot = copy.deepcopy(self._tables)
            try:
                yield
            except Exception:
                self._tables = snapshot
                raise

    @contextmanager
    def locked(self) -> Iterator[None]:
        """只读口径的加锁入口：与写事务互斥，但不做快照、不参与回滚。

        列表、详情这类读取需要按水位回算执行状态与行数，期间要避免与写事务的
        回滚换表并发，又没必要承担整库深拷贝成本。
        """
        with self._lock:
            yield

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()

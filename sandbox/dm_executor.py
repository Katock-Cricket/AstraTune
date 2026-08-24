from typing import Any, Dict, List, Optional

from sandbox.executor import DBExecutor


class DMExecutor(DBExecutor):
    """达梦数据库执行器（已冻结，不在评测范围内）。"""

    def __init__(self, config: Dict[str, Any]):
        raise NotImplementedError("达梦适配已冻结，不再维护")

    def connect(self) -> None:
        raise NotImplementedError("达梦适配已冻结，不再维护")

    def execute(self, sql: str, fetch: bool = True) -> Optional[List[Dict[str, Any]]]:
        raise NotImplementedError("达梦适配已冻结，不再维护")

    def close(self) -> None:
        raise NotImplementedError("达梦适配已冻结，不再维护")

    def table_exists(self, table_name: str) -> bool:
        raise NotImplementedError("达梦适配已冻结，不再维护")

    def get_table_count(self, table_name: str) -> int:
        raise NotImplementedError("达梦适配已冻结，不再维护")

    def get_create_table_ddl(self, table_name: str) -> str:
        raise NotImplementedError("达梦适配已冻结，不再维护")

"""Verify Agent 可用的沙箱工具（reset 等）。"""

from __future__ import annotations

from typing import Callable, Optional


def create_verify_reset_tool(reset_fn: Callable[..., str]):
    """
    基于 Verify 管线的 reset_fn（cleanup+setup）构造 LangChain 工具。
    reset_fn 须支持 force=True 跳过 smart reset。
    """

    def reset_sandbox_environment(reason: str) -> str:
        """
        全量重建沙箱（清空后从目标库重新拷贝/采样表）。

        **仅当同时满足**才调用：
        1. 规则信号显示沙箱基表/ schema 被不可逆破坏；
        2. 题面 clean_up_sql 无法恢复；
        3. 不 reset 会导致本轮或后续候选 Verify 不可信。

        禁止预防性 reset；Verify 每轮最多调用一次。

        Args:
            reason: 中文说明（必填，指出哪条规则/错误表明环境已毁）

        Returns:
            重建结果
        """
        r = (reason or "").strip()
        if len(r) < 8:
            return (
                "拒绝 reset：请说明哪条候选/规则错误表明沙箱不可逆损坏，"
                "且 clean_up 无法恢复。"
            )
        try:
            outcome = reset_fn(force=True)
            return f"沙箱已强制 full reset（outcome={outcome}）。请重新评判候选。"
        except Exception as e:
            return f"沙箱 reset 失败: {e}"

    return reset_sandbox_environment

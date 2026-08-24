"""沙箱相对 target 的还原度清单：供 Diag / Generate / Verify 透明使用。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def empty_fidelity(
    *,
    dialect: str = "postgresql",
    slot_db: str = "",
) -> Dict[str, Any]:
    return {
        "dialect": dialect,
        "slot_db": slot_db or "",
        "copy_threshold": None,
        "sample_size": None,
        "preprocess_applied": False,
        "tables": [],
        "warnings": [
            "采样表上的结果集 Soft EX 可能与生产不一致；"
            "统计注入失败时 QEP/cost 不可与生产直接对比。"
        ],
    }


def format_fidelity_text(fidelity: Optional[Dict[str, Any]]) -> str:
    """短自然语言 + 要点列表，注入 Agent 上下文。"""
    if not fidelity:
        return "（无沙箱保真信息）"
    lines: List[str] = []
    dialect = fidelity.get("dialect") or "?"
    slot = fidelity.get("slot_db") or "?"
    lines.append(f"方言={dialect}；槽位库={slot}")
    if fidelity.get("copy_threshold") is not None:
        lines.append(
            f"拷贝阈值={fidelity.get('copy_threshold')}；"
            f"采样上限={fidelity.get('sample_size')}"
        )
    lines.append(
        f"preprocess 已 apply={'是' if fidelity.get('preprocess_applied') else '否/未知'}"
    )
    tables = fidelity.get("tables") or []
    if not tables:
        lines.append("表清单：空（抽表失败或尚未搭建）")
    else:
        lines.append("表清单：")
        for t in tables:
            mode = "采样" if t.get("is_sampled") else "全量拷贝"
            method = t.get("sample_method") or ("full" if not t.get("is_sampled") else "sample")
            lines.append(
                f"- {t.get('resolved') or t.get('original')}: "
                f"target_rows={t.get('original_rows')} "
                f"sandbox_rows={t.get('sandbox_rows')} "
                f"mode={mode}({method})"
            )
            stats = t.get("stats") or {}
            ok = stats.get("ok") or []
            fail = stats.get("fail") or []
            skip = stats.get("skip") or []
            if ok:
                lines.append(f"  统计成功: {', '.join(ok[:12])}" + ("…" if len(ok) > 12 else ""))
            if fail:
                lines.append(f"  统计失败: {', '.join(fail[:8])}" + ("…" if len(fail) > 8 else ""))
            if skip:
                lines.append(f"  统计跳过: {', '.join(skip[:6])}")
    for w in fidelity.get("warnings") or []:
        lines.append(f"[注意] {w}")
    return "\n".join(lines)


def mark_preprocess_applied(fidelity: Optional[Dict[str, Any]], applied: bool = True) -> Dict[str, Any]:
    out = dict(fidelity or empty_fidelity())
    out["preprocess_applied"] = bool(applied)
    return out

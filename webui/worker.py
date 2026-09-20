"""WebUI 单题子进程：注入 overlay configs，强制采样沙箱。"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _append_event(path: Path, name: str, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(
        {"ts": _now(), "name": name, "payload": payload or {}},
        ensure_ascii=False,
    )
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def main() -> int:
    if len(sys.argv) < 2:
        sys.stderr.write("webui.worker: 缺少 payload 路径\n")
        return 2
    payload_path = Path(sys.argv[1])
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    events_path = Path(payload["events_path"])
    result_path = Path(payload["result_path"])

    from webui.overlay import resolve_env_key_aliases

    for src, dest in resolve_env_key_aliases(payload.get("agents_for_env") or {}):
        val = (os.environ.get(src) or "").strip()
        if val and not (os.environ.get(dest) or "").strip():
            os.environ[dest] = val

    from utils.llm import batch_llm_pool_options, init_llm_pool
    from utils.inference_runner import run_single_instance

    configs = payload["configs"]
    agents = configs.get("agents") or {}
    batch_cfg = agents.get("batch") or {}
    pool_opts = batch_llm_pool_options(batch_cfg, cli_llm_workers=1)
    diag_llm = ((agents.get("diagnosis") or {}).get("llm") or {})
    opt_llm = ((agents.get("optimization") or {}).get("llm") or {})
    cfg_key = diag_llm.get("api_key") or opt_llm.get("api_key")
    init_llm_pool(cfg_key=cfg_key, **pool_opts)

    def on_phase(name: str, data: dict) -> None:
        _append_event(events_path, name, data)

    _append_event(events_path, "started", {"slot_index": payload.get("slot_index")})

    rec = payload["task"]
    result = run_single_instance(
        sql=rec.get("issue_sql") or [],
        query=rec.get("query") or "",
        category=rec.get("category") or "Query",
        dialect=rec.get("dialect") or "postgresql",
        db_id=rec.get("db_id"),
        preprocess_sql="\n".join(rec.get("preprocess_sql") or []) or None,
        clean_up_sql="\n".join(rec.get("clean_up_sql") or []) or None,
        original_schema=str(rec.get("original_schema") or ""),
        mode=rec.get("mode") or "diag+opt",
        function=rec.get("function") or "general",
        stream=False,
        slot_index=payload.get("slot_index"),
        workers=int(payload.get("workers") or 1),
        instance_id=str(rec.get("instance_id") or rec.get("id") or "-"),
        configs=configs,
        benchmark=None,
        direct_target=False,
        on_phase=on_phase,
    )
    out = {
        "pred_sqls": list(result.pred_sqls or []),
        "diagnosis_report": result.diagnosis_report or "",
        "error": result.error,
        "elapsed_ms": result.elapsed_ms,
        "meta": result.meta,
        "tables": result.tables,
    }
    result_path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    if result.error and not any(str(x).strip() for x in (result.pred_sqls or [])):
        _append_event(events_path, "failed", {"error": result.error})
        return 1
    _append_event(events_path, "finished", {"n_pred": len(out["pred_sqls"])})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

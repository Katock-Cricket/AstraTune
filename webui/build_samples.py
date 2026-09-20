"""生成 WebUI 样例 JSONL（实现一次性；结果提交 git）。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.benchmark_data import FLASH_INPUT, POST_INPUT, ROOT
from webui.import_jsonl import GOLD_KEYS

OUT_DIR = ROOT / "data" / "webui"
GENERAL_N = 20
SLOW_N = 20


def _load(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rows.append(json.loads(line))
    return rows


def _strip(row: dict, *, slow: bool) -> dict:
    out = {k: v for k, v in row.items() if k not in GOLD_KEYS}
    out["is_slow_sql"] = slow
    # 样例不带巨型 schema
    out.pop("original_schema", None)
    out.pop("preprocess_schema", None)
    keep = [
        "instance_id",
        "db_id",
        "dialect",
        "query",
        "issue_sql",
        "preprocess_sql",
        "clean_up_sql",
        "category",
        "is_slow_sql",
    ]
    return {k: out[k] for k in keep if k in out}


def _is_eff(row: dict) -> bool:
    return str(row.get("category") or "").strip().lower() == "efficiency"


def pick_general(rows: list[dict], n: int) -> list[dict]:
    buckets = {"Query": [], "Management": [], "Personalization": []}
    rest = []
    for r in rows:
        if _is_eff(r):
            continue
        if not r.get("issue_sql"):
            continue
        cat = str(r.get("category") or "Query")
        if cat in buckets:
            buckets[cat].append(r)
        else:
            rest.append(r)
    picked: list[dict] = []
    # 轮询三类
    idxs = {k: 0 for k in buckets}
    while len(picked) < n:
        progressed = False
        for cat in ("Query", "Management", "Personalization"):
            i = idxs[cat]
            if i < len(buckets[cat]):
                picked.append(buckets[cat][i])
                idxs[cat] = i + 1
                progressed = True
                if len(picked) >= n:
                    break
        if not progressed:
            break
    if len(picked) < n:
        for r in rest:
            picked.append(r)
            if len(picked) >= n:
                break
    return [_strip(r, slow=False) for r in picked[:n]]


def pick_slow(rows: list[dict], n: int) -> list[dict]:
    cands = [r for r in rows if _is_eff(r) and r.get("issue_sql")]
    # 优先覆盖不同 db_id
    by_db: dict[str, list] = {}
    for r in cands:
        by_db.setdefault(str(r.get("db_id") or ""), []).append(r)
    picked = []
    while len(picked) < n and any(by_db.values()):
        for db in list(by_db.keys()):
            if not by_db[db]:
                continue
            picked.append(by_db[db].pop(0))
            if len(picked) >= n:
                break
    return [_strip(r, slow=True) for r in picked[:n]]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    post = _load(POST_INPUT)
    flash = _load(FLASH_INPUT)
    general = pick_general(post, GENERAL_N)
    slow = pick_slow(flash, SLOW_N)
    if len(general) != GENERAL_N:
        raise SystemExit(f"通用样例不足: {len(general)}")
    if len(slow) != SLOW_N:
        raise SystemExit(f"慢 SQL 样例不足: {len(slow)}（Flash Efficiency 题数不够）")
    gp = OUT_DIR / "sample_general.jsonl"
    sp = OUT_DIR / "sample_slow.jsonl"
    gp.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in general), encoding="utf-8")
    sp.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in slow), encoding="utf-8")
    print(f"wrote {gp} ({len(general)})")
    print(f"wrote {sp} ({len(slow)}) dbs={sorted({r['db_id'] for r in slow})}")


if __name__ == "__main__":
    main()

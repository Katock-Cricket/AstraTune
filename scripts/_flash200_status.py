"""Flash200 pred 进度：流式读 JSONL，避免整文件 read_text 卡死。"""
import json
from pathlib import Path

p = Path("outputs/flash200_pred.jsonl")
if not p.is_file():
    print("no pred file")
    raise SystemExit(0)
by = {}
with p.open(encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        iid = r.get("instance_id")
        if iid is not None:
            by[iid] = r
ok = fb = hf = empty = 0
for r in by.values():
    m = r.get("optimization_meta") or {}
    if m.get("success"):
        ok += 1
    if m.get("fallback"):
        fb += 1
    if m.get("hard_fail"):
        hf += 1
    pred = r.get("pred_sqls") or []
    if not any(str(x).strip() for x in pred):
        empty += 1
size_mb = p.stat().st_size / (1024 * 1024)
print(f"unique={len(by)} ok={ok} fb={fb} hf={hf} empty={empty} size_mb={size_mb:.1f}")

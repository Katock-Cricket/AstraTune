"""批量推理前置检查：磁盘空间、单实例锁、Docker 可用性。"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple


def _d_drive_free_gb() -> Optional[float]:
    if sys.platform != "win32":
        try:
            usage = shutil.disk_usage("/")
            return usage.free / (1024**3)
        except Exception:
            return None
    try:
        import ctypes

        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(
            ctypes.c_wchar_p("D:\\"), None, None, ctypes.pointer(free_bytes)
        )
        return free_bytes.value / (1024**3)
    except Exception:
        return None


def find_running_batch_pids(script_name: str = "run_bird_critic.py") -> list[int]:
    if sys.platform != "win32":
        return []
    try:
        out = subprocess.check_output(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"Get-CimInstance Win32_Process | Where-Object {{ $_.CommandLine -like '*{script_name}*' }} | Select-Object -ExpandProperty ProcessId",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        pids = []
        for line in out.splitlines():
            line = line.strip()
            if line.isdigit():
                pids.append(int(line))
        return pids
    except Exception:
        return []


def acquire_batch_lock(lock_path: Path, *, force: bool = False) -> Tuple[bool, str]:
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.is_file():
        try:
            old = lock_path.read_text(encoding="utf-8").strip()
        except Exception:
            old = "(unknown)"
        if not force:
            return False, f"已有批次锁 {lock_path}（{old}）。勿重复开跑；加 --force-batch 可覆盖。"
    lock_path.write_text(f"pid={os.getpid()}", encoding="utf-8")
    return True, "ok"


def release_batch_lock(lock_path: Path) -> None:
    try:
        Path(lock_path).unlink(missing_ok=True)
    except Exception:
        pass


def docker_pg_ready(host: str = "127.0.0.1", port: int = 5433) -> bool:
    try:
        import socket

        with socket.create_connection((host, port), timeout=2):
            return True
    except Exception:
        return False


def preflight_batch_inference(
    *,
    min_free_gb: float = 30.0,
    abort_free_gb: float = 2.0,
    lock_path: Optional[Path] = None,
    force_batch: bool = False,
    check_docker: bool = True,
) -> None:
    """不通过则 SystemExit。"""
    free = _d_drive_free_gb()
    if free is not None:
        if free < abort_free_gb:
            raise SystemExit(
                f"D: 剩余 {free:.2f} GB < {abort_free_gb} GB，磁盘已满，"
                f"请先 scripts/compact_docker_vhdx.ps1 回收空间后再跑批。"
            )
        if free < min_free_gb:
            print(
                f"WARNING: D: 剩余 {free:.2f} GB < 建议 {min_free_gb} GB，"
                f"大批量推理可能卡死或 Docker 500。",
                file=sys.stderr,
            )

    pids = find_running_batch_pids()
    others = [p for p in pids if p != os.getpid()]
    if others and not force_batch:
        raise SystemExit(
            f"检测到其它 run_bird_critic 进程: {others}。"
            f"请先结束残留进程或使用 --force-batch（慎用）。"
        )

    if lock_path:
        ok, msg = acquire_batch_lock(lock_path, force=force_batch)
        if not ok:
            raise SystemExit(msg)

    if check_docker and not docker_pg_ready():
        print(
            "WARNING: PG 沙箱 5433 不可达，推理可能全部 Execution Error。",
            file=sys.stderr,
        )

    if not force_batch:
        try:
            from utils.config_loader import load_all_configs, get_batch_config, get_diagnosis_config
            from utils.llm import batch_llm_pool_options, probe_llm_gateway

            configs = load_all_configs()
            llm_cfg = (get_diagnosis_config(configs).get("agent") or {}).get("llm") or {}
            key_mode = str(get_batch_config(configs).get("llm_key_mode") or "auto")
            ok, msg = probe_llm_gateway(llm_cfg, key_mode=key_mode)
            if not ok:
                raise SystemExit(f"LLM 网关不可用，中止批跑: {msg}")
        except SystemExit:
            raise
        except Exception as e:
            print(f"WARNING: LLM 网关探测跳过: {e}", file=sys.stderr)

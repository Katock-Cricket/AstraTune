import { useCallback, useEffect, useMemo, useState } from "react";
import { api, type Connection, type Task, type TaskStatus } from "../api";
import TaskDetail from "./TaskDetail";

const STATUS_LABEL: Record<TaskStatus, string> = {
  pending: "待执行",
  running: "执行中",
  completed: "已完成",
  failed: "失败",
  cancelled: "已取消",
};

function badge(status: TaskStatus) {
  const map: Record<TaskStatus, string> = {
    pending: "bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300",
    running: "bg-cyan-100 text-cyan-800 dark:bg-cyan-950 dark:text-cyan-300",
    completed: "bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300",
    failed: "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300",
    cancelled: "bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300",
  };
  return (
    <span className={`rounded px-2 py-0.5 text-xs ${map[status]}`}>{STATUS_LABEL[status]}</span>
  );
}

export default function Tasks() {
  const [items, setItems] = useState<Task[]>([]);
  const [conns, setConns] = useState<Connection[]>([]);
  const [status, setStatus] = useState("");
  const [slow, setSlow] = useState("");
  const [q, setQ] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [detailId, setDetailId] = useState<string | null>(null);
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const [showCreate, setShowCreate] = useState(false);

  const load = useCallback(async () => {
    const params: Record<string, string | boolean | undefined> = {
      status: status || undefined,
      q: q || undefined,
    };
    if (slow === "1") params.is_slow_sql = true;
    if (slow === "0") params.is_slow_sql = false;
    const data = await api.tasks(params);
    setItems(data.items);
  }, [status, slow, q]);

  useEffect(() => {
    api.targets().then((r) => setConns(r.connections)).catch(() => undefined);
  }, []);

  useEffect(() => {
    load().catch((e) => setErr(String(e.message || e)));
    const id = setInterval(() => load().catch(() => undefined), 3000);
    return () => clearInterval(id);
  }, [load]);

  const allIds = useMemo(() => items.map((t) => t.id), [items]);
  const toggleAll = () => {
    if (selected.size === items.length) setSelected(new Set());
    else setSelected(new Set(allIds));
  };

  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    setErr("");
    try {
      await fn();
      await load();
    } catch (e) {
      setErr(String((e as Error).message || e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <h1 className="mr-auto text-lg font-semibold">任务</h1>
        <button className="btn" disabled={busy} onClick={() => setShowCreate(true)}>
          导入单条
        </button>
        <label className="btn cursor-pointer">
          导入 JSONL
          <input
            type="file"
            accept=".jsonl,.json"
            className="hidden"
            onChange={(e) => {
              const f = e.target.files?.[0];
              e.target.value = "";
              if (!f) return;
              act(async () => {
                const r = await api.importJsonl(f);
                if (r.failed) setErr(`导入成功 ${r.created}，失败 ${r.failed}`);
              });
            }}
          />
        </label>
        <button
          className="btn-primary"
          disabled={busy}
          onClick={() =>
            act(() => api.start(selected.size ? [...selected] : undefined))
          }
        >
          开始执行
        </button>
        <button className="btn" disabled={busy} onClick={() => act(() => api.export(selected.size ? [...selected] : undefined))}>
          导出
        </button>
      </div>

      <div className="flex flex-wrap gap-2">
        <select className="field" value={status} onChange={(e) => setStatus(e.target.value)}>
          <option value="">全部状态</option>
          {Object.entries(STATUS_LABEL).map(([k, v]) => (
            <option key={k} value={k}>
              {v}
            </option>
          ))}
        </select>
        <select className="field" value={slow} onChange={(e) => setSlow(e.target.value)}>
          <option value="">全部类型</option>
          <option value="1">慢 SQL</option>
          <option value="0">通用 SQL</option>
        </select>
        <input
          className="field w-56"
          placeholder="搜索 id / 库 / 描述"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
      </div>

      {err && <div className="rounded border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-900 dark:bg-red-950 dark:text-red-200">{err}</div>}

      {items.length === 0 ? (
        <div className="rounded-lg border border-dashed border-slate-300 p-10 text-center text-sm text-slate-500 dark:border-slate-700">
          暂无任务。可导入单条，或使用 <span className="mono">data/webui/sample_general.jsonl</span> /{" "}
          <span className="mono">sample_slow.jsonl</span>。
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
          <table className="min-w-full text-left text-sm">
            <thead className="border-b border-slate-200 bg-slate-50 text-xs uppercase text-slate-500 dark:border-slate-800 dark:bg-slate-950">
              <tr>
                <th className="p-3">
                  <input type="checkbox" checked={selected.size === items.length && items.length > 0} onChange={toggleAll} />
                </th>
                <th className="p-3">状态</th>
                <th className="p-3">类型</th>
                <th className="p-3">实例</th>
                <th className="p-3">数据库</th>
                <th className="p-3">方言</th>
                <th className="p-3">连接</th>
                <th className="p-3">模式</th>
                <th className="p-3">耗时</th>
                <th className="p-3">更新</th>
                <th className="p-3" />
              </tr>
            </thead>
            <tbody>
              {items.map((t) => (
                <tr
                  key={t.id}
                  className="cursor-pointer border-b border-slate-100 hover:bg-slate-50 dark:border-slate-800 dark:hover:bg-slate-800/60"
                  onClick={() => setDetailId(t.id)}
                >
                  <td className="p-3" onClick={(e) => e.stopPropagation()}>
                    <input
                      type="checkbox"
                      checked={selected.has(t.id)}
                      onChange={() => {
                        const n = new Set(selected);
                        if (n.has(t.id)) n.delete(t.id);
                        else n.add(t.id);
                        setSelected(n);
                      }}
                    />
                  </td>
                  <td className="p-3">{badge(t.status)}</td>
                  <td className="p-3">{t.is_slow_sql ? "慢 SQL" : "通用"}</td>
                  <td className="p-3 mono">{t.instance_id}</td>
                  <td className="p-3">{t.db_id}</td>
                  <td className="p-3">{t.dialect}</td>
                  <td className="p-3">{t.connection_name || t.connection_id}</td>
                  <td className="p-3 mono text-xs">
                    {t.mode}/{t.function}
                  </td>
                  <td className="p-3">{t.elapsed_ms != null ? `${Math.round(t.elapsed_ms)} ms` : "—"}</td>
                  <td className="p-3 text-xs text-slate-500">{t.updated_at?.slice(0, 19)}</td>
                  <td className="p-3" onClick={(e) => e.stopPropagation()}>
                    {t.status === "running" ? (
                      <button className="text-xs text-red-600" onClick={() => act(() => api.cancel(t.id))}>
                        中断
                      </button>
                    ) : (
                      <button
                        className="text-xs text-slate-500 hover:text-red-600"
                        onClick={() => act(() => api.remove(t.id))}
                      >
                        删除
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {detailId && <TaskDetail id={detailId} onClose={() => setDetailId(null)} />}
      {showCreate && (
        <CreateModal
          conns={conns}
          onClose={() => setShowCreate(false)}
          onCreated={() => {
            setShowCreate(false);
            load();
          }}
        />
      )}
    </div>
  );
}

function CreateModal({
  conns,
  onClose,
  onCreated,
}: {
  conns: Connection[];
  onClose: () => void;
  onCreated: () => void;
}) {
  const [connection_id, setCid] = useState(conns[0]?.id || "");
  const [db_id, setDb] = useState("financial");
  const [issue_sql, setSql] = useState("");
  const [query, setQuery] = useState("");
  const [is_slow_sql, setSlow] = useState(false);
  const [preprocess_sql, setPre] = useState("");
  const [clean_up_sql, setCu] = useState("");
  const [err, setErr] = useState("");

  return (
    <div className="fixed inset-0 z-40 flex items-start justify-center bg-black/40 p-8">
      <div className="w-full max-w-2xl rounded-lg bg-white p-5 shadow-xl dark:bg-slate-900">
        <h2 className="mb-4 text-base font-semibold">导入单条</h2>
        {err && <div className="mb-3 text-sm text-red-600">{err}</div>}
        <div className="grid grid-cols-2 gap-3">
          <label className="text-sm">
            目标连接
            <select className="field mt-1 w-full" value={connection_id} onChange={(e) => setCid(e.target.value)}>
              {conns.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name} ({c.dialect})
                </option>
              ))}
            </select>
          </label>
          <label className="text-sm">
            数据库名 (db_id)
            <input className="field mt-1 w-full" value={db_id} onChange={(e) => setDb(e.target.value)} />
          </label>
          <label className="col-span-2 text-sm">
            问题 SQL
            <textarea className="field mt-1 h-28 w-full mono" value={issue_sql} onChange={(e) => setSql(e.target.value)} />
          </label>
          <label className="col-span-2 text-sm">
            问题描述
            <textarea className="field mt-1 h-20 w-full" value={query} onChange={(e) => setQuery(e.target.value)} />
          </label>
          <label className="text-sm">
            前置 SQL
            <textarea className="field mt-1 h-16 w-full mono" value={preprocess_sql} onChange={(e) => setPre(e.target.value)} />
          </label>
          <label className="text-sm">
            后置 SQL
            <textarea className="field mt-1 h-16 w-full mono" value={clean_up_sql} onChange={(e) => setCu(e.target.value)} />
          </label>
          <label className="flex items-center gap-2 text-sm">
            <input type="checkbox" checked={is_slow_sql} onChange={(e) => setSlow(e.target.checked)} />
            慢 SQL
          </label>
        </div>
        <div className="mt-4 flex justify-end gap-2">
          <button className="btn" onClick={onClose}>
            取消
          </button>
          <button
            className="btn-primary"
            onClick={async () => {
              try {
                await api.createTask({
                  connection_id,
                  db_id,
                  issue_sql,
                  query,
                  is_slow_sql,
                  preprocess_sql,
                  clean_up_sql,
                });
                onCreated();
              } catch (e) {
                setErr(String((e as Error).message || e));
              }
            }}
          >
            保存
          </button>
        </div>
      </div>
    </div>
  );
}

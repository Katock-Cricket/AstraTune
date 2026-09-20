import { useEffect, useState } from "react";
import { api, type PhaseEvent, type Task } from "../api";

const PHASE: Record<string, string> = {
  queued: "排队",
  started: "启动",
  extract_tables: "抽表",
  sandbox_setup: "沙箱",
  diagnosis: "诊断",
  intent_spec: "Intent Spec",
  opt_round: "Generate–Verify",
  select_final: "选定 pred",
  finished: "完成",
  failed: "失败",
  cancelled: "已取消",
};

export default function TaskDetail({ id, onClose }: { id: string; onClose: () => void }) {
  const [task, setTask] = useState<Task | null>(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    let stop = false;
    const tick = async () => {
      try {
        const t = await api.task(id);
        if (!stop) setTask(t);
      } catch (e) {
        if (!stop) setErr(String((e as Error).message || e));
      }
    };
    tick();
    const n = setInterval(tick, 1500);
    return () => {
      stop = true;
      clearInterval(n);
    };
  }, [id]);

  const events: PhaseEvent[] = task?.events || [];

  return (
    <div className="fixed inset-0 z-40 flex justify-end bg-black/30">
      <div className="flex h-full w-full max-w-3xl flex-col bg-white shadow-2xl dark:bg-slate-900">
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-3 dark:border-slate-800">
          <div>
            <div className="text-sm font-semibold">{task?.instance_id || id}</div>
            <div className="text-xs text-slate-500">
              {task?.db_id} · {task?.function} · {task?.mode}
            </div>
          </div>
          <button className="btn" onClick={onClose}>
            关闭
          </button>
        </div>
        <div className="flex-1 space-y-5 overflow-auto p-5">
          {err && <div className="text-sm text-red-600">{err}</div>}
          {!task && <div className="text-sm text-slate-500">加载中…</div>}
          {task && (
            <>
              {task.error && (
                <div className="rounded border border-red-200 bg-red-50 p-3 text-sm dark:border-red-900 dark:bg-red-950">
                  {task.error}
                </div>
              )}
              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">阶段时间线</h3>
                {events.length === 0 ? (
                  <div className="text-sm text-slate-500">尚无阶段事件。</div>
                ) : (
                  <ol className="space-y-2 border-l border-slate-200 pl-4 dark:border-slate-700">
                    {events.map((e) => (
                      <li key={e.id}>
                        <div className="text-sm font-medium">{PHASE[e.name] || e.name}</div>
                        <div className="text-xs text-slate-500">{e.ts}</div>
                        {e.payload && Object.keys(e.payload).length > 0 && (
                          <pre className="mt-1 max-h-32 overflow-auto rounded bg-slate-50 p-2 text-xs dark:bg-slate-950">
                            {JSON.stringify(e.payload, null, 2)}
                          </pre>
                        )}
                      </li>
                    ))}
                  </ol>
                )}
              </section>
              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">诊断报告</h3>
                <pre className="max-h-64 overflow-auto whitespace-pre-wrap rounded bg-slate-50 p-3 text-xs dark:bg-slate-950">
                  {task.diagnosis_report || "（空）"}
                </pre>
              </section>
              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">pred_sqls</h3>
                {task.pred_sqls?.length ? (
                  task.pred_sqls.map((s, i) => (
                    <pre key={i} className="mb-2 overflow-auto rounded bg-slate-50 p-3 text-xs dark:bg-slate-950">
                      {s}
                    </pre>
                  ))
                ) : (
                  <div className="text-sm text-slate-500">尚无结果。</div>
                )}
              </section>
              {task.optimization_meta && (
                <details>
                  <summary className="cursor-pointer text-xs text-slate-500">optimization_meta</summary>
                  <pre className="mt-2 max-h-64 overflow-auto rounded bg-slate-50 p-3 text-xs dark:bg-slate-950">
                    {JSON.stringify(task.optimization_meta, null, 2)}
                  </pre>
                </details>
              )}
              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">问题 SQL</h3>
                <pre className="overflow-auto rounded bg-slate-50 p-3 text-xs dark:bg-slate-950">
                  {(task.issue_sql || []).join(";\n")}
                </pre>
              </section>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

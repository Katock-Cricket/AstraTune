export type TaskStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

export type Connection = {
  id: string;
  name: string;
  dialect: string;
  host: string;
  port: number;
  user: string;
  password: string;
  note: string;
  is_default: boolean;
};

export type PhaseEvent = {
  id: number;
  task_id: string;
  ts: string;
  name: string;
  payload: Record<string, unknown>;
};

export type Task = {
  id: string;
  created_at: string;
  updated_at: string;
  instance_id: string;
  connection_id: string;
  connection_name?: string;
  db_id: string;
  dialect: string;
  issue_sql: string[];
  preprocess_sql: string[];
  clean_up_sql: string[];
  query: string;
  category: string;
  is_slow_sql: boolean;
  function: string;
  mode: string;
  status: TaskStatus;
  pred_sqls: string[];
  diagnosis_report: string;
  error: string | null;
  elapsed_ms: number | null;
  pid: number | null;
  slot_index: number | null;
  optimization_meta: Record<string, unknown> | null;
  events?: PhaseEvent[];
};

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok) {
    let msg = res.statusText;
    try {
      const body = await res.json();
      msg = body.message || body.detail || JSON.stringify(body);
    } catch {
      /* ignore */
    }
    throw new Error(msg);
  }
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("json")) return (await res.json()) as T;
  return (await res.text()) as T;
}

export const api = {
  health: () => req<{ ok: boolean }>("/api/health"),
  targets: () => req<{ connections: Connection[] }>("/api/config/targets"),
  putTargets: (connections: Connection[]) =>
    req<{ connections: Connection[] }>("/api/config/targets", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ connections }),
    }),
  testTarget: (id: string, db_id?: string) =>
    req<{ ok: boolean; message: string }>(`/api/config/targets/${id}/test`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ db_id: db_id || null }),
    }),
  sandbox: () => req<{ base: unknown; overlay: Record<string, unknown>; effective: Record<string, unknown> }>("/api/config/sandbox"),
  putSandbox: (overlay: Record<string, unknown>) =>
    req("/api/config/sandbox", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ overlay }),
    }),
  testSandbox: (dialect: string) =>
    req<{ ok: boolean; message: string }>(`/api/config/sandbox/test?dialect=${encodeURIComponent(dialect)}`, {
      method: "POST",
    }),
  agents: () =>
    req<{
      base: Record<string, unknown>;
      overlay: Record<string, unknown>;
      effective: Record<string, unknown>;
      workers: number;
    }>("/api/config/agents"),
  putAgents: (overlay: Record<string, unknown>, workers?: number) =>
    req("/api/config/agents", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ overlay, workers }),
    }),
  tasks: (params: Record<string, string | boolean | undefined> = {}) => {
    const q = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v === undefined || v === "") return;
      q.set(k, String(v));
    });
    const s = q.toString();
    return req<{ items: Task[]; workers: number; running: number }>(`/api/tasks${s ? `?${s}` : ""}`);
  },
  task: (id: string) => req<Task>(`/api/tasks/${id}`),
  events: (id: string, after = 0) =>
    req<{ events: PhaseEvent[] }>(`/api/tasks/${id}/events?after_id=${after}`),
  createTask: (body: Record<string, unknown>) =>
    req<Task>("/api/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  importJsonl: (file: File) => {
    const fd = new FormData();
    fd.append("file", file);
    return req<{ created: number; failed: number; failures: { line: number; reason: string; excerpt: string }[] }>(
      "/api/tasks/import",
      { method: "POST", body: fd }
    );
  },
  start: (ids?: string[]) =>
    req<{ started: string[]; skipped: { id: string; reason: string }[] }>("/api/tasks/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ids: ids || null }),
    }),
  cancel: (id: string) => req(`/api/tasks/${id}/cancel`, { method: "POST" }),
  remove: (id: string) => req(`/api/tasks/${id}`, { method: "DELETE" }),
  export: async (ids?: string[]) => {
    const res = await fetch("/api/tasks/export", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ids: ids || null }),
    });
    if (!res.ok) throw new Error("导出失败");
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "astratune_pred.jsonl";
    a.click();
    URL.revokeObjectURL(url);
  },
};

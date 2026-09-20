import { useEffect, useState } from "react";
import { api, type Connection } from "../api";

type Tab = "targets" | "sandbox" | "agents";

function getPath(obj: Record<string, unknown>, path: string[]): unknown {
  let cur: unknown = obj;
  for (const p of path) {
    if (!cur || typeof cur !== "object") return undefined;
    cur = (cur as Record<string, unknown>)[p];
  }
  return cur;
}

function setPath(obj: Record<string, unknown>, path: string[], value: unknown) {
  let cur: Record<string, unknown> = obj;
  path.forEach((p, i) => {
    if (i === path.length - 1) {
      cur[p] = value;
      return;
    }
    const next = cur[p];
    if (!next || typeof next !== "object") cur[p] = {};
    cur = cur[p] as Record<string, unknown>;
  });
}

export default function Settings() {
  const [tab, setTab] = useState<Tab>("targets");
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">系统配置</h1>
      <div className="mb-4 flex gap-1 border-b border-slate-200 dark:border-slate-800">
        {(
          [
            ["targets", "目标库"],
            ["sandbox", "沙箱"],
            ["agents", "Agent"],
          ] as [Tab, string][]
        ).map(([k, lab]) => (
          <button
            key={k}
            className={`px-4 py-2 text-sm ${tab === k ? "border-b-2 border-cyan-600 font-medium" : "text-slate-500"}`}
            onClick={() => setTab(k)}
          >
            {lab}
          </button>
        ))}
      </div>
      {tab === "targets" && <TargetsPane />}
      {tab === "sandbox" && <SandboxPane />}
      {tab === "agents" && <AgentsPane />}
    </div>
  );
}

function TargetsPane() {
  const [rows, setRows] = useState<Connection[]>([]);
  const [msg, setMsg] = useState("");
  const load = () => api.targets().then((r) => setRows(r.connections));
  useEffect(() => {
    load().catch((e) => setMsg(String(e.message || e)));
  }, []);

  const save = async () => {
    setMsg("");
    await api.putTargets(rows);
    setMsg("已保存（仅 overlay，未写仓库 config）");
  };

  return (
    <div className="space-y-3">
      {msg && <div className="text-sm text-slate-500">{msg}</div>}
      {rows.map((c, i) => (
        <div key={c.id} className="grid grid-cols-6 gap-2 rounded-lg border border-slate-200 p-3 dark:border-slate-800">
          <input className="field" value={c.name} onChange={(e) => (rows[i] = { ...c, name: e.target.value }, setRows([...rows]))} />
          <select
            className="field"
            value={c.dialect}
            onChange={(e) => (rows[i] = { ...c, dialect: e.target.value }, setRows([...rows]))}
          >
            <option value="postgresql">postgresql</option>
            <option value="mysql">mysql</option>
          </select>
          <input className="field" value={c.host} onChange={(e) => (rows[i] = { ...c, host: e.target.value }, setRows([...rows]))} />
          <input
            className="field"
            type="number"
            value={c.port}
            onChange={(e) => (rows[i] = { ...c, port: Number(e.target.value) }, setRows([...rows]))}
          />
          <input className="field" value={c.user} onChange={(e) => (rows[i] = { ...c, user: e.target.value }, setRows([...rows]))} />
          <input
            className="field"
            type="password"
            value={c.password}
            onChange={(e) => (rows[i] = { ...c, password: e.target.value }, setRows([...rows]))}
          />
          <div className="col-span-6 flex gap-2">
            <button
              className="btn"
              onClick={async () => {
                const r = await api.testTarget(c.id);
                setMsg(r.ok ? `${c.name}: 连接成功` : `${c.name}: ${r.message}`);
              }}
            >
              测试连接
            </button>
            <button className="btn" onClick={() => setRows(rows.filter((_, j) => j !== i))}>
              删除
            </button>
          </div>
        </div>
      ))}
      <button
        className="btn"
        onClick={() =>
          setRows([
            ...rows,
            {
              id: `conn-${Date.now()}`,
              name: "新连接",
              dialect: "postgresql",
              host: "127.0.0.1",
              port: 5432,
              user: "root",
              password: "",
              note: "",
              is_default: false,
            },
          ])
        }
      >
        添加连接
      </button>
      <button className="btn-primary" onClick={() => save().catch((e) => setMsg(String(e.message || e)))}>
        保存
      </button>
    </div>
  );
}

function SandboxPane() {
  const [effective, setEffective] = useState<Record<string, unknown>>({});
  const [msg, setMsg] = useState("");
  const [text, setText] = useState("");

  const load = async () => {
    const r = await api.sandbox();
    setEffective(r.effective);
    setText(JSON.stringify(r.overlay, null, 2));
  };
  useEffect(() => {
    load().catch((e) => setMsg(String(e.message || e)));
  }, []);

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <div>
        <h3 className="mb-2 text-sm font-medium">生效配置</h3>
        <pre className="max-h-[480px] overflow-auto rounded bg-slate-50 p-3 text-xs dark:bg-slate-950">
          {JSON.stringify(effective, null, 2)}
        </pre>
        <div className="mt-2 flex gap-2">
          <button className="btn" onClick={() => api.testSandbox("postgresql").then((r) => setMsg(r.message))}>
            测试 PG 沙箱
          </button>
          <button className="btn" onClick={() => api.testSandbox("mysql").then((r) => setMsg(r.message))}>
            测试 MySQL 沙箱
          </button>
        </div>
        {msg && <div className="mt-2 text-sm text-slate-500">{msg}</div>}
      </div>
      <div>
        <h3 className="mb-2 text-sm font-medium">Overlay（JSON）</h3>
        <textarea className="field h-[420px] w-full mono text-xs" value={text} onChange={(e) => setText(e.target.value)} />
        <button
          className="btn-primary mt-2"
          onClick={async () => {
            const o = JSON.parse(text || "{}");
            await api.putSandbox(o);
            await load();
            setMsg("沙箱 overlay 已保存");
          }}
        >
          保存 overlay
        </button>
      </div>
    </div>
  );
}

function AgentsPane() {
  const [overlay, setOverlay] = useState<Record<string, unknown>>({});
  const [effective, setEffective] = useState<Record<string, unknown>>({});
  const [workers, setWorkers] = useState(2);
  const [msg, setMsg] = useState("");

  const load = async () => {
    const r = await api.agents();
    setOverlay(r.overlay || {});
    setEffective(r.effective || {});
    setWorkers(r.workers);
  };
  useEffect(() => {
    load().catch((e) => setMsg(String(e.message || e)));
  }, []);

  const llm = (role: string[]) => (getPath(overlay, role) as Record<string, unknown>) || {};

  const patchLlm = (role: string[], field: string, value: unknown) => {
    const next = structuredClone(overlay);
    setPath(next, [...role, field], value);
    setOverlay(next);
  };

  const Role = ({ title, path }: { title: string; path: string[] }) => {
    const L = llm(path);
    return (
      <div className="rounded-lg border border-slate-200 p-4 dark:border-slate-800">
        <h3 className="mb-3 text-sm font-semibold">{title}</h3>
        <div className="grid grid-cols-2 gap-2 text-sm">
          <label>
            base_url
            <input className="field mt-1 w-full" value={String(L.base_url ?? getPath(effective, path.concat("base_url")) ?? "")} onChange={(e) => patchLlm(path, "base_url", e.target.value)} />
          </label>
          <label>
            model
            <input className="field mt-1 w-full" value={String(L.model ?? getPath(effective, path.concat("model")) ?? "")} onChange={(e) => patchLlm(path, "model", e.target.value)} />
          </label>
          <label>
            api_key 来源
            <select
              className="field mt-1 w-full"
              value={String(L.api_key_source || "env")}
              onChange={(e) => patchLlm(path, "api_key_source", e.target.value)}
            >
              <option value="env">环境变量</option>
              <option value="direct">直接输入</option>
            </select>
          </label>
          <label>
            环境变量名
            <input
              className="field mt-1 w-full"
              value={String(L.api_key_env || "OPENAI_API_KEY")}
              onChange={(e) => patchLlm(path, "api_key_env", e.target.value)}
            />
          </label>
          <label className="col-span-2">
            api_key（直接输入；已保存则留空表示不改）
            <input
              className="field mt-1 w-full"
              type="password"
              placeholder={L.api_key_set ? "已保存" : ""}
              value={String(L.api_key || "")}
              onChange={(e) => patchLlm(path, "api_key", e.target.value)}
            />
          </label>
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-4">
      {msg && <div className="text-sm text-slate-500">{msg}</div>}
      <label className="text-sm">
        WebUI 并行 workers（1–4）
        <input
          className="field ml-2 w-20"
          type="number"
          min={1}
          max={4}
          value={workers}
          onChange={(e) => setWorkers(Number(e.target.value))}
        />
      </label>
      <Role title="诊断" path={["diagnosis", "llm"]} />
      <Role title="Generate" path={["optimization", "llm"]} />
      <Role title="Verify" path={["optimization", "verify", "llm"]} />
      <button
        className="btn-primary"
        onClick={async () => {
          await api.putAgents(overlay, workers);
          await load();
          setMsg("Agent overlay 已保存");
        }}
      >
        保存
      </button>
    </div>
  );
}

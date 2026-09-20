import { NavLink, Route, Routes } from "react-router-dom";
import { useEffect, useState } from "react";
import Tasks from "./pages/Tasks";
import Settings from "./pages/Settings";
import { api } from "./api";

const THEME_KEY = "astratune-theme";

function applyTheme(mode: string) {
  const dark =
    mode === "dark" ||
    (mode === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.classList.toggle("dark", dark);
}

export default function App() {
  const [theme, setTheme] = useState(() => localStorage.getItem(THEME_KEY) || "system");
  const [running, setRunning] = useState(0);
  const [workers, setWorkers] = useState(2);

  useEffect(() => {
    applyTheme(theme);
    localStorage.setItem(THEME_KEY, theme);
  }, [theme]);

  useEffect(() => {
    const tick = async () => {
      try {
        const t = await api.tasks();
        setRunning(t.running);
        setWorkers(t.workers);
      } catch {
        /* ignore */
      }
    };
    tick();
    const id = setInterval(tick, 4000);
    return () => clearInterval(id);
  }, []);

  const link = ({ isActive }: { isActive: boolean }) =>
    `block rounded-md px-3 py-2 text-sm ${
      isActive
        ? "bg-cyan-700 text-white"
        : "text-slate-300 hover:bg-slate-800 hover:text-white"
    }`;

  return (
    <div className="flex min-h-screen">
      <aside className="flex w-52 shrink-0 flex-col bg-slate-900 text-slate-100">
        <div className="border-b border-slate-800 px-4 py-4">
          <div className="text-sm font-semibold tracking-wide">AstraTune</div>
          <div className="mt-1 text-xs text-slate-400">SQL 诊断与修复</div>
        </div>
        <nav className="flex-1 space-y-1 p-3">
          <NavLink to="/" end className={link}>
            任务
          </NavLink>
          <NavLink to="/settings" className={link}>
            配置
          </NavLink>
        </nav>
      </aside>
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-12 items-center justify-between border-b border-slate-200 bg-white px-5 dark:border-slate-800 dark:bg-slate-900">
          <div className="text-sm text-slate-500">
            并行 {workers} · 执行中 {running}
          </div>
          <select
            className="rounded border border-slate-300 bg-transparent px-2 py-1 text-sm dark:border-slate-700"
            value={theme}
            onChange={(e) => setTheme(e.target.value)}
          >
            <option value="system">跟随系统</option>
            <option value="light">浅色</option>
            <option value="dark">深色</option>
          </select>
        </header>
        <main className="min-w-0 flex-1 overflow-auto p-5">
          <Routes>
            <Route path="/" element={<Tasks />} />
            <Route path="/settings" element={<Settings />} />
          </Routes>
        </main>
      </div>
    </div>
  );
}

# AstraTune WebUI 规格

> **状态**：规格已拍板，**实现已落地**（2026-09-14）  
> **日期**：2026-09-14  
> **关联**：需求原稿 `docs/AstraTune WebUI 需求文档.md`；机制 `docs/架构说明书.md`；进度 `docs/过程文档.md`；入口约定 `AGENTS.md`  
> **权威性**：本文件是 WebUI 的实现规格。与需求原文冲突时 **以本文件为准**。

---

## 0. 定位与边界

WebUI 是 AstraTune 的 **本地演示与操作台**，用来展示：

- **主**：慢 SQL 修复（`function=efficiency`）
- **次**：通用 SQL 修复（`function=general`）

端到端交付仍是 `pred_sqls: list[str]`。推理核心 **只** 走 `utils/inference_runner.py::run_single_instance()`，与 `scripts/infer.py` 同一条链路。

**库路径（硬约束）**：


| 路径                                                                 | WebUI 是否使用                                                  |
| ------------------------------------------------------------------ | ----------------------------------------------------------- |
| AstraTune 目标库（PG `:5432` / MySQL `:3306`）+ 采样沙箱（`:5433` / `:3307`） | **唯一执行路径**                                                  |
| BIRD 官方评测 ephemeral copy                                           | 不用                                                          |
| Diff-SQL / Effi 评测库（`:7001` / `:5438`）                             | **不用**；不设 `direct_target=True`，不读 `config/effi_db.json` 做推理 |


三套 Docker（AstraTune `docker/`、BIRD `benchmark/evaluation`、Diff-SQL）**保持独立，WebUI 不合并 compose**。

WebUI **不是** 全量跑批器，也 **不** 在界面里触发官方 BIRD / Effi eval。

---



## 1. 已拍板决策


| #   | 决策               | 说明                                                                                     |
| --- | ---------------- | -------------------------------------------------------------------------------------- |
| W1  | 一期做完整台           | 配置（目标库 / 沙箱 / Agent）+ 任务队列 + 导入导出 + 阶段轨迹                                               |
| W2  | 慢 SQL = 采样沙箱     | 与 PG530 相同：从已配置目标库抽表克隆；**禁止** Effi `direct_target`                                     |
| W3  | 有界并行             | 默认 `workers=2`，配置页可改 **1～4**；中断 = kill 对应子进程                                           |
| W4  | SQLite overlay   | 任务 + 轨迹 + 运行时配置落本地库；**禁止写回** 仓库 `config/*.json`                                        |
| W5  | 导入自动选职能          | 默认 `mode=diag+opt`；慢 SQL → `function=efficiency`；通用 → `general`；手工单条可改                 |
| W6  | 导出统一 `pred_sqls` | 不做 Effi `prediction` 双格式；不做官方 eval                                                     |
| W7  | 阶段时间线            | 抽表 / 沙箱 / 诊断 / Intent Spec / 每轮 Generate–Verify / 终态 `pred_sqls`；**不追 token 流**        |
| W8  | 视觉               | 正式商用数据产品；**取消洛丽塔 / 小粉毛 / 任何吉祥物**；中文；dark / light                                       |
| W9  | 样例               | **PG530 风格** JSONL：20 条通用 + 20 条慢 SQL；慢 SQL 从 **Flash 200 的 Efficiency**（共 22 题）抽 20 题 |
| W10 | 方言               | PostgreSQL + MySQL 均要接执行                                                               |
| W11 | 前端               | Vite + React + TypeScript + Tailwind；FastAPI **同端口** 托管静态资源                            |
| W12 | 鉴权               | 无登录，本机单用户                                                                              |
| W13 | 入口               | `python at_webui.py` 起本地网页；生产由后端托管前端 `dist`                                            |


---



## 2. 信息架构

两块主工作区（侧栏），不要 demo 向导风、不要分步卡通引导。

```text
┌────────┬──────────────────────────────────────┐
│ AstraTune │ 顶栏：主题切换 / workers / 运行中数量   │
│          ├──────────────────────────────────────┤
│ 任务     │ 列表 | 导入 | 执行 | 筛选              │
│ 配置     │ 目标库 | 沙箱 | Agent                  │
└────────┴──────────────────────────────────────┘
点击任务行 → 右侧抽屉或独立页：阶段时间线 + 报告 + pred_sqls
```



### 2.1 任务

- 列表列：状态、是否慢 SQL、`instance_id`、`db_id`、方言、连接名、`mode`/`function`、耗时、更新时间
- 筛选：状态（待执行 / 执行中 / 已完成 / 失败 / 已取消）、是否慢 SQL、方言、关键词（id / db / 描述）
- 操作：导入单条、导入 JSONL、开始执行（勾选或全部待执行）、中断、删除、批量导出
- 空态 / 错误态 / 执行中态都要有正式文案，不要占位插画吉祥物



### 2.2 配置 · 目标库

- 多条 **具名连接**（不止按方言各一条）
- 字段：显示名、方言、host、port、user、password、备注
- `db_id` **不是** 连接字段：它是该实例上的 **数据库名**，跟在每条任务上（与 CLI `--db-id` 一致）
- 支持测试连接（选一个 `db_id` 或连 `postgres`/`mysql` 元库）
- 首次启动：从 `config/target_db.json` **只读灌入** 两条默认连接（PG / MySQL），之后只改 overlay



### 2.3 配置 · 沙箱

- 按方言各一套：host / port / user / password、`copy_thr`、`sample_size`、`sampling_strategy`、`sampling_params`、`template_cache`
- 展示当前生效值 = `config/sandbox.json` ⊕ overlay
- 测试连接
- 「查看」= 生效配置 + 连通性；不要求在 UI 里枚举全部槽位库名



### 2.4 配置 · Agent

三个角色分开配，对应 `config/agents.json`：


| 角色       | 配置键                                                             |
| -------- | --------------------------------------------------------------- |
| 诊断       | `diagnosis.llm.*`、`diagnosis.max_iter`                          |
| Generate | `optimization.llm.*`、`optimization.max_rounds`、`num_candidates` |
| Verify   | `optimization.verify.llm.*`、`verify_mode`、`exec_timeout_ms`     |


LLM 公共字段：`base_url`、`model`、超时分层（`timeout` / `first_token_timeout` / `response_timeout`）、`max_attempts`、`temperature`。

**api_key**：

- `source=env`：填写环境变量名（默认 `OPENAI_API_KEY`；兼容 `OPENAI_API_KEYS`）
- `source=direct`：明文写入 overlay（仅本机 SQLite，不进 git）
- UI 对已保存的 direct key 掩码显示

另：WebUI 专用 `workers`（1～4，默认 2），**不要** 把批跑 `batch.max_workers=16` 暴露成 WebUI 默认。

---



## 3. 配置 Overlay

```text
config/*.json          ← 仓库默认，WebUI 只读
        ↓ merge
data/webui/webui.sqlite  ← overlay + 任务 + 轨迹（gitignore）
        ↓
run_single_instance(configs=merged)
```

规则：

1. 禁止 `open("config/agents.json", "w")` 一类写回。
2. Overlay 缺字段时回落到仓库 JSON。
3. 合并结果只存在于进程 / 子进程 payload，不改磁盘上的 `config/`。
4. SQLite 路径：`data/webui/webui.sqlite`（相对仓库根）。

---



## 4. 任务模型



### 4.1 字段


| 字段                                | 说明                                                        |
| --------------------------------- | --------------------------------------------------------- |
| `id`                              | WebUI 内部 UUID                                             |
| `instance_id`                     | 展示 / 导出键；导入沿用题面 id，手工导入可自动生成                              |
| `connection_id`                   | 具名目标连接                                                    |
| `db_id`                           | 目标实例上的数据库名                                                |
| `dialect`                         | `postgresql` / `mysql`                                    |
| `issue_sql`                       | `list[str]` 或单字符串（入库统一 list）                              |
| `preprocess_sql` / `clean_up_sql` | 可空                                                        |
| `query`                           | 问题描述（对应 CLI `--user-prompt`）                              |
| `category`                        | Query / Management / Personalization / Efficiency         |
| `is_slow_sql`                     | bool；UI 分类用                                               |
| `function`                        | `general` / `efficiency`                                  |
| `mode`                            | 默认 `diag+opt`；手工可改为 `diag_only` / `opt_only` / `llm_only` |
| `status`                          | 见状态机                                                      |
| `pred_sqls`                       | 完成后写入                                                     |
| `diagnosis_report`                | 可空                                                        |
| `error`                           | 失败 / 取消原因                                                 |
| `elapsed_ms`                      | 墙钟                                                        |
| `pid`                             | 运行中子进程 pid                                                |


**导入时必须剥掉**：`sol_sql`、`test_cases`、一切 gold。推理路径不得看见它们。

### 4.2 状态机

```text
pending ──start──► running ──成功（有 pred_sqls）──► completed
                      │
                      ├─超时 / 推理 error ──► failed
                      └─用户中断 kill ──► cancelled

pending / failed / cancelled / completed 可删除
running 必须先中断再删
failed / cancelled 可再次 start → pending 语义下重跑（清空 pred 与轨迹）
```

「修复完成」= `completed`（含 Verify fallback 仍交付了 `pred_sqls` 的情况，与批跑「有 pred 不清空」一致）。

---



## 5. 导入与样例



### 5.1 单条导入（表单）

必填：目标连接、`db_id`、`issue_sql`、是否慢 SQL。  
选填：描述、preprocess、cleanup、`category`、`mode`。

是否慢 SQL = true → `function=efficiency` 且若 `category` 空则设为 `Efficiency`。  
false → `function=general`，`category` 默认 `Query`。

### 5.2 JSONL 批量导入

接受 BIRD 风格行（与 `run_bird_critic.run_one` 输入对齐）：

`instance_id`, `db_id`, `dialect`, `issue_sql`, `query`, `preprocess_sql`, `clean_up_sql`, `category`, `original_schema`（可有可无）

自动规则：


| 题面信号                                                                                   | 结果                                          |
| -------------------------------------------------------------------------------------- | ------------------------------------------- |
| `category=Efficiency` 或 `efficiency=true` 或 `function=efficiency` 或 `is_slow_sql=true` | 慢 SQL；`function=efficiency`；`mode=diag+opt` |
| 其他                                                                                     | 通用；`function=general`；`mode=diag+opt`       |


`connection_id`：若 JSONL 无此字段，按 `dialect` 匹配默认具名连接；匹配不到则该行失败并在导入报告中列出，不静默丢弃。

非法行（无 `issue_sql`）计入报告，不中断整批。

### 5.3 随仓库样例（实现时生成，提交 git）


| 文件                                | 内容                                                                                                                             |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `data/webui/sample_general.jsonl` | **20** 条通用 SQL，从 **PG 530**（`benchmark_data/input/post/` ⊕ schema）抽取；Query / Management / Personalization 混取；**不含** Efficiency |
| `data/webui/sample_slow.jsonl`    | **20** 条慢 SQL，从 **Flash 200** 中 `category=Efficiency` 的 22 题抽 20 题                                                             |


抽取约束：

1. 删除 `sol_sql` / `test_cases`。
2. 显式写入 `"is_slow_sql": false|true`。
3. 保留 `db_id`（须为 AstraTune `pg_target` 上已有库，如 `financial`）。
4. **不要** 用 Effi-SQL 题面 / Interact / TPC-H。
5. 慢 SQL 20 题优先覆盖不同 `db_id`，不足再重复库。

---



## 6. 执行引擎

父进程：FastAPI 调度器。  
子进程：每条任务一个进程，调用 `run_single_instance`（可复用 `scripts/run_bird_worker.py` 思路，但必须注入 **merged overlay configs**，且 `direct_target=False`**、**`benchmark` **不得为** `effi`）。


| 项   | 规格                                                                       |
| --- | ------------------------------------------------------------------------ |
| 并行  | 同时 `running` 数 ≤ `workers`（1～4）                                          |
| 槽位  | `slot_index` 绑定在飞 worker，避免共享可写沙箱                                        |
| 排队  | 仅点过「开始执行」的任务会拉起（`queued`）；刚导入的 pending 不自动跑；槽满则继续 pending |
| 中断  | 杀进程组（Windows / POSIX 都要能杀掉子进程）                                           |
| 超时  | 用 overlay 或 `batch.instance_timeout_s`（默认 2400）；到期强杀 → `failed`          |
| 同库  | WebUI 规模小，**不** 强制同 `db_id` 串行（与 Effi 批跑不同）；仍禁止与 CLI 全量跑批同时打同一沙箱（文档警告即可） |


调度器 / 状态迁移 / workers 钳制必须是可单测的纯函数（见 §15）。

---



## 7. 推理接入

`run_single_instance` 增加 **可选** `on_phase(name: str, payload: dict) -> None`，默认 `None`。

- CLI / 现有批跑 **行为不变**（不传 callback）。
- WebUI worker 传入 callback：把阶段事件追加到 SQLite（或任务旁路 JSONL，父进程读库即可）。
- **禁止** 把逻辑写进 `infer.py` 壳；批跑 worker 壳也不堆业务。

强制参数：

```text
direct_target=False
benchmark=None 或 "bird"
function / mode / category 来自任务行
configs=overlay 合并结果
stream=False     # 不走 Rich token 流
```

Efficiency + 采样沙箱：沿用现机制（Soft EX / `eff_risk`；**QEP 不硬否决**）。不要为 WebUI 另开一套 Verify。

---



## 8. 轨迹（阶段时间线）

样式参考成熟 agentic 控制台（阶段条 + 右侧详情），但是 **阶段事件**，不是逐 token。


| `name`                 | 何时                                                                    |
| ---------------------- | --------------------------------------------------------------------- |
| `queued`               | 进入待跑                                                                  |
| `started`              | 子进程起来                                                                 |
| `extract_tables`       | 抽表结束（payload：表名）                                                      |
| `sandbox_setup`        | 沙箱 setup 结束（payload：fidelity 摘要）                                      |
| `diagnosis`            | 诊断开始 / 结束（结束时带报告长度，不要求全文增量）                                           |
| `intent_spec`          | Spec 抽取结束                                                             |
| `opt_round`            | Generate–Verify 每一轮开始 / 结束（payload：round、候选数、是否通过、failure_reasons 摘要） |
| `select_final`         | 选出 `pred_sqls`                                                        |
| `finished`             | 成功                                                                    |
| `failed` / `cancelled` | 终态                                                                    |


执行中打开任务详情：轮询或 SSE 拉已落库事件；完成后只读。  
详情页同时展示：诊断报告全文、`pred_sqls`、`optimization_meta` 中与轮次相关的摘要（`round_history` 可折叠）。

---



## 9. 导出

仅 `completed`（可选勾选 failed 不含 pred 的不导出，或导出但 `pred_sqls=[]` 并带 `status`——默认 **只导出有非空** `pred_sqls` **的任务**）。

每行 JSONL 字段（统一 AstraTune 交付，不用 Effi `prediction`）：

```text
instance_id, db_id, dialect, category, is_slow_sql, function, mode,
issue_sql, query, pred_sqls, diagnosis_report, elapsed_ms, status
```

批量导出为浏览器下载。不写 `outputs/` 除非用户另存。

---



## 10. HTTP API（FastAPI）

基址 `/api`。本机绑定 `127.0.0.1:8080`。


| 方法      | 路径                              | 作用                       |
| ------- | ------------------------------- | ------------------------ |
| GET/PUT | `/api/config/targets`           | 具名连接列表                   |
| POST    | `/api/config/targets/{id}/test` | 测连                       |
| GET/PUT | `/api/config/sandbox`           | 沙箱 overlay               |
| GET/PUT | `/api/config/agents`            | Agent overlay + workers  |
| POST    | `/api/tasks`                    | 单条导入                     |
| POST    | `/api/tasks/import`             | JSONL 上传                 |
| GET     | `/api/tasks`                    | 列表 + 筛选                  |
| GET     | `/api/tasks/{id}`               | 详情                       |
| GET     | `/api/tasks/{id}/events`        | 阶段事件（可用 SSE）             |
| POST    | `/api/tasks/start`              | `{ids?:[]}` 空则全部 pending |
| POST    | `/api/tasks/{id}/cancel`        | 中断                       |
| DELETE  | `/api/tasks/{id}`               | 删除                       |
| POST    | `/api/tasks/export`             | 返回 jsonl 文件              |


错误用 JSON `{code, message}`，HTTP 4xx/5xx。不要 HTML 错误页给 API。

静态：`/` 起 React SPA；未匹配路由回 `index.html`。

---



## 11. 前端


| 项   | 规格                                                                          |
| --- | --------------------------------------------------------------------------- |
| 栈   | Vite 5 + React 18 + TypeScript + Tailwind CSS                               |
| 组件  | 可用 shadcn/ui（Radix）做表格、对话框、表单；保持克制、密信息、商用后台                                 |
| 主题  | CSS 变量；`html.dark`；默认跟随系统，可手动切                                              |
| 色   | 中性灰 + 一条品牌强调色（蓝或青均可）；**禁止粉色洛丽塔色盘、花边、手写体标题、吉祥物**                             |
| 文案  | 中文                                                                          |
| 布局  | 桌面优先（≥1280）；不必做独立移动站，但窄屏不要横向撑爆                                              |
| 开发  | Vite `:5173` 代理 `/api` → FastAPI；`at_webui.py` 生产模式托管 `webui/frontend/dist` |


---



## 12. 目录与入口

```text
at_webui.py                 # 薄入口：uvicorn webui.app:app --host 127.0.0.1 --port 8080
webui/
  app.py                    # FastAPI 装配
  overlay.py                # 配置合并（纯函数）
  store.py                  # SQLite
  task_state.py             # 状态机 / 槽位（纯函数）
  scheduler.py              # 有界子进程调度
  worker.py                 # 子进程入口
  import_jsonl.py           # 导入映射（纯函数）
  schemas.py                # Pydantic
  frontend/                 # Vite 工程
data/webui/
  sample_general.jsonl
  sample_slow.jsonl
  webui.sqlite              # 运行时，不提交
```

`at_webui.py` 现在仓库里不存在（过程文档曾写「空文件」）。本规格要求 **新建薄入口**，不要在入口堆业务。

依赖：`requirements.txt` 增加 `fastapi`、`uvicorn[standard]`、`python-multipart`（上传 JSONL）。前端依赖锁在 `webui/frontend/`。

---



## 13. 非功能

1. 交互按成熟商用控制台（表、筛选、批量操作、详情钻取），不要「演示向导」。
2. 本地单用户；不鉴权；只绑 `127.0.0.1`。
3. 密钥不进 git；overlay SQLite gitignore。
4. 中文 UI。
5. dark / light。
6. 一条 `python at_webui.py` 可起（需已 `conda activate at`，前端需已 build；开发模式可 `AT_WEBUI_DEV=1` 只起 API 并另开 Vite）。
7. 与 CLI 批跑隔离：WebUI 挂了不得污染 `config/*.json`。

---



## 14. 明确不做

- RAG / Reflect / 微调 / 达梦
- 写回 `config/*.json`
- Diff-SQL compose 合并进 AstraTune Docker
- `direct_target` / Effi `:7001` `:5438`
- 界面内官方评测（BIRD wrapper / `run_effi_eval`）
- 多用户、登录、公网暴露
- 追 LLM token 的直播工作流
- 洛丽塔视觉 / 吉祥物
- 把 WebUI 做成 16 worker 全量跑批替代品（上限 4）

---



## 15. 测试（无 DB / 无 LLM key）

实现时新增，例如 `tests/test_webui_overlay.py`、`tests/test_webui_import.py`、`tests/test_webui_queue.py`。锁定：

1. overlay 合并：缺字段回落仓库 JSON；有 overlay 覆盖对应键；**不写** `config/` 路径。
2. 导入：Efficiency → `is_slow_sql` + `function=efficiency`；剥 `sol_sql`/`test_cases`；无 `issue_sql` 计入失败报告。
3. `workers` 钳制在 1～4。
4. 状态机：running 不可直接删；cancel 才到 `cancelled`；completed 导出含 `pred_sqls`。
5. 样例 JSONL：各 20 行；无 gold 字段；慢 SQL 全部 `is_slow_sql=true`。
6. `on_phase=None` 时 `run_single_instance` 现有单测仍过（现有 `tests/test_inference_modes.py` 等不得红）。

DoD：代码 + 上述测试绿 + 本文档与架构/过程/环境搭建对齐。

---



## 16. 实现顺序

1. 纯函数：overlay / import / 状态机 / workers 钳制 + 测试
2. SQLite store + FastAPI 配置与任务 CRUD
3. `on_phase` 接到 `inference_runner` + 子进程 worker + 有界队列
4. 样例 JSONL
5. 前端：壳 + 配置三页 + 任务列表/详情/导入导出 + 主题
6. `at_webui.py` 托管静态资源；环境搭建补启动命令

---



## 17. 与现架构的衔接要点

- 目标库具名连接在运行时映射为今日 `target_db` 段：`host/port/user/password`，再用任务 `db_id` 覆盖 `database`。
- 沙箱仍按方言分段，与 `SandboxManager` 一致。
- 慢 SQL 在 WebUI 里 **不是** Effi 专项栈；只是 `function=efficiency` 的采样沙箱推理（主演示叙事）。官方慢 SQL 成绩仍以 `docs/effi300评测报告.md` 为准，与本 UI 脱钩。
- Flash 慢 SQL 样例仅作题面；执行仍打 AstraTune `pg_target` 上同名 `db_id`。


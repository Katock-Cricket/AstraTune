# AstraTune WebUI 需求文档

本文档是 WebUI 的需求原稿。 **实现规格以 [`docs/spec_webui.md`](spec_webui.md) 为准**；冲突时听规格。

本 WebUI 用于演示 AstraTune 的慢 SQL 修复能力（主要）与通用 SQL 修复能力（次要）。支持连接目标数据库、查看与配置沙箱、配置 Agent、导入问题 SQL、管理修复进度、查看单例阶段轨迹、导出 `pred_sqls`。

推理一律走 AstraTune **目标库采样克隆沙箱**（与 `infer.py` / PG530 相同）。**不**接入 Diff-SQL / Effi 评测库，**不**在界面跑官方 eval。

### 连接目标数据库

系统配置中可配置 **多条具名** 目标连接（IP、端口、用户名、密码、方言），支持测试连接。任务上的 `db_id` 是该实例上的数据库名。

### 查看与配置沙箱数据库

系统配置中查看并修改沙箱连接、采样阈值与策略等。生效值为仓库 `config/sandbox.json` ⊕ 运行时 overlay（不写回仓库 JSON）。

### 配置 Agent

系统配置中分别配置诊断 / Generate / Verify 的 LLM URL、api_key（明文或从环境变量读取）、model、超时、最大尝试次数、最大迭代轮数等。

### 导入问题SQL

任务执行支持单条导入（目标连接、`db_id`、问题 SQL、前置 / 后置 SQL、描述、是否慢 SQL、`mode`）。也支持 JSONL 批量导入。随仓库样例为 **PG530 风格**：20 条通用 + 20 条慢 SQL（慢 SQL 从 Flash 200 的 Efficiency 抽取）。导入自动：慢 SQL → `function=efficiency`，否则 `general`；默认 `mode=diag+opt`。

### 管理总体修复进度

查看待执行 / 执行中 / 已完成（及失败、已取消）。支持删除、中断、开始执行、按是否慢 SQL 筛选。默认并行 2，可配 1～4；中断杀子进程。

### 查看单例修复轨迹

点击执行中或已完成的任务，查看 **阶段时间线**（抽表 / 沙箱 / 诊断 / Intent Spec / 各轮 Generate–Verify / `pred_sqls`）。不追 token 直播。

### 管理修复结果

已完成任务可批量导出 JSONL（统一字段 `pred_sqls`）。

## 非功能需求

1. 界面与交互按成熟商用控制台；不要 demo 向导感
2. **取消**洛丽塔 / 小粉毛 / 吉祥物；正式数据产品视觉
3. 前端：Vite + React + TypeScript + Tailwind；FastAPI 同端口托管
4. 后端 FastAPI；单条工作流只调用 `run_single_instance()`
5. dark / light
6. 中文
7. 本地网页：`python at_webui.py`（`127.0.0.1:8080`），无登录
8. 配置与任务落 SQLite overlay，禁止写回 `config/*.json`

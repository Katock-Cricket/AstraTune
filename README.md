# AstraTune
基于大模型的卫星互联网数据库性能诊断与优化

## 安装依赖
```shell
conda create -n at python=3.10
conda activate at
pip install uv
uv pip install -r requirements.txt
```

## 开发进度

- [x] 25.11：开题报告
- [x] 26.01：连接本地MySQL数据库+通用基模，搭建含沙箱工具的诊断智能体Demo
- [x] 26.01.13：修复工具调用问题，实现流式输出，支持PostgreSQL
- [ ] 搭建诊断Neo4J知识图谱，知识抽取+关系识别+索引+结构化存储
- [ ] 集成RAG工具到诊断智能体
- [ ] …

## 基本使用

1. configs配置业务数据库(target_db)、沙箱数据库(sandbox)、智能体(agents)
2. 执行诊断脚本：详见测试数据.md示例


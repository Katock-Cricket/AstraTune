# AstraTune
基于大模型的卫星互联网数据库性能诊断与优化

## 安装依赖
pip install -r requirements.txt

## 基本使用
python diag.py \
  --sql "SELECT * FROM users WHERE age > 20 ORDER BY created_at" \
  --tables "users" \
  --log "平均执行时间: 2.5s, 执行次数: 1000次/天"

## 多表诊断
python diag.py \
  --sql "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 20" \
  --tables "users,orders" \
  --log "平均执行时间: 5s"

## 指定数据库
python diag.py \
  --sql "SELECT * FROM products WHERE category_id = 1" \
  --tables "products" \
  --database "test_db"

## 输出日志到文件
python diag.py \
  --sql "SELECT * FROM users WHERE age > 20" \
  --tables "users" \
  --log-file "diagnosis.log"

## 配置文件说明

### configs/agents.json
- llm: LLM配置（base_url, model, api_key, temperature）
- max_iter: 最大迭代次数
- enable_rag: 是否启用RAG检索（当前为接口）
- enable_test: 是否启用测试模式（预留）

### configs/sandbox.json
- dialect: 数据库类型（当前支持mysql）
- host, port, user, password: 数据库连接信息
- copy_thr: 完整拷贝阈值（行数）
- sample_size: 采样大小
- sampling_strategy: 采样策略（random/time_based/stratified）
- sampling_params: 采样参数（预留）

## 注意事项
1. 确保MySQL数据库已启动并可访问
2. 配置文件中的数据库连接信息需正确
3. 相关表必须存在于数据库中
4. 系统会自动创建和清理沙箱表（表名_sandbox）


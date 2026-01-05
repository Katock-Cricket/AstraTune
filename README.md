# AstraTune
基于大模型的卫星互联网数据库性能诊断与优化

## 安装依赖
```shell
conda create -n at python=3.11
conda activate at
pip install uv
uv pip install -r requirements.txt
```

## 开发进度

- [x] 25.11：开题报告
- [x] 26.05：连接本地MySQL数据库+通用基模，搭建含沙箱工具的诊断智能体Demo
- [ ] 搭建诊断Neo4J知识图谱，知识抽取+关系识别+索引+结构化存储
- [ ] 集成RAG工具到诊断智能体
- [ ] …

## 基本使用

1. configs配置业务数据库(target_db)、沙箱数据库(sandbox)、智能体(agents)
2. 执行诊断脚本：

```shell
python diag.py --sql "SET @current_time = TIMESTAMP('2022-12-01 12:40:00');WITH cte AS (SELECT slot, SUM(total) OVER(ORDER BY slot) AS total, total AS rowtotal FROM sales WHERE slot < @current_time ORDER BY slot DESC LIMIT 1) SELECT total - (30 - TIMESTAMPDIFF(MINUTE, slot, @current_time))/30 * rowtotal AS total FROM cte" --tables "customers" --preprocess-sql "CREATE TABLE sales (id INT, slot TIMESTAMP, total INT);INSERT INTO sales VALUES(1, '2022-12-01T12:00', 100), (2, '2022-12-01T12:30', 150), (3, '2022-12-01T13:00', 200);CREATE INDEX idx_test ON sales (slot);" --clean-up-sql "SET @current_time := NULL;DROP TABLE sales;" --user-prompt "Suppose we have a transactions data table within an e-commerce platform that records purchases made by various customers at different gas stations. The table 'sales' is like |id|slot|total|
There's an index on slot already. I want to sum the total up to the current moment in time (EDIT: WASN\'T CLEAR INITIALLY, I WILL PROVIDE A LOWER SLOT BOUND, SO THE SUM WILL BE OVER SOME NUMBER OF DAYS/WEEKS, NOT OVER FULL TABLE). Let\'s say the time is currently 2022-12-01T12:45. If I run select * from my_table where slot < CURRENT_TIMESTAMP(), then I get back records 1 and 2. However, in my data, the records represent forecasted sales within a time slot. I want to find the forecasts as of 2022-12-01T12:45, and so I want to find the proportion of the half hour slot of record 2 that has elapsed, and return that proportion of the total. As of 2022-12-01T12:45 (assuming minute granularity), 50% of row 2 has elapsed, so I would expect the total to return as 150 / 2 = 75. My current query works, but is slow. What are some ways I can optimise this, or other approaches I can take? Also, how can we extend this solution to be generalised to any interval frequency? Maybe tomorrow we change our forecasting model and the data comes in sporadically. The hardcoded 30 would not work in that case.The platform tracks the sales forecasts for gas products, which are recorded in half-hour time slots. Due to recent platform updates, users want to calculate the total forecasted sales up to the current moment in time, taking into account the proportion of the current half-hour slot that has elapsed."
```


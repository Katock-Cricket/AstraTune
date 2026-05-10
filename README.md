# AstraTune

基于大模型的卫星互联网数据库性能诊断与优化

## 安装依赖

```shell
conda create -n at python=3.10
conda activate at
pip install uv
uv pip install -r requirements.txt
```

## 基本使用

1. 起docker容器，默认数据库都在本地：沙箱、目标数据库（PG）、同时开放一层API给benchmark用。

```
cd scripts

# 第一次运行，构建docker
.\start_docker_stack.ps1 -Build
# 若已构建，重复运行，则不带Build
.\start_docker_stack.ps1
```

1. configs配置业务数据库(target_db)、沙箱数据库(sandbox)、智能体(agents)
2. 执行诊断脚本：详见测试数据.md示例

## 运行Benchmark

```

```

